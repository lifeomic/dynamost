import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { z } from 'zod';
import _pick from 'lodash/pick';
import retry from 'async-retry';
import {
  DynamoDBCondition,
  DynamoDBUpdate,
  KeyCondition,
  serializeCondition,
  serializeKeyCondition,
  serializeUpdate,
} from './dynamo-expressions';
import { batchWrite } from './batch-write';

/* -- Utility types to support indexing + other top-level config --  */
export type KeySchema<Entity> = {
  hash: keyof Entity;
  range: keyof Entity | undefined;
};

type RoughConfig<Entity> = {
  /** The name of the table. */
  tableName: string;
  /** The key schema for the table. */
  keys: KeySchema<Entity>;
  /** Any secondary indexes for the table. */
  secondaryIndexes: { [key: string]: KeySchema<Entity> };
};

type CompleteKeyForIndex<Entity, Keys extends KeySchema<Entity>> = {
  [Key in Keys['hash']]: Entity[Keys['hash']];
} & undefined extends Keys['range']
  ? {}
  : {
      [Key in NonNullable<Keys['range']>]: string;
    };

export type GetOptions = {
  consistentRead?: boolean;
};

export type PutOptions<Item> = {
  /** Whether to allow overwriting existing records. Defaults to `false`. */
  overwrite?: boolean;
  /** A condition for the write. */
  condition?: DynamoDBCondition<Item>;
};

export type PatchOptions<Item> = {
  /** A condition for the write. */
  condition?: DynamoDBCondition<Item>;
};

export type DeleteOptions<Item> = {
  /** A condition for the write. */
  condition?: DynamoDBCondition<Item>;
};

/* Types for particular methods */
export type QueryOptions = {
  /** The maximum number of records to retrieve. */
  limit?: number;
  /**
   * Whether to scan the index in ascending order. Defaults to `true`.
   */
  scanIndexForward?: boolean;
  /**
   * A page token from a previous query. If provided, the query will
   * resume from where the previous query left off.
   */
  nextPageToken?: string;
  /**
   * Whether to perform a consistent query. Only valid when querying
   * the main table.
   */
  consistentRead?: boolean;
};

export type QueryResponse<Entity> = {
  items: Entity[];
  nextPageToken?: string;
};

export type DeleteAllOptions = Omit<QueryOptions, 'limit' | 'nextPageToken'>;

export const PageToken = {
  encode: (data?: unknown) =>
    data ? Buffer.from(JSON.stringify(data)).toString('base64') : undefined,
  decode: (token?: string): any =>
    token
      ? JSON.parse(Buffer.from(token, 'base64').toString('utf8'))
      : undefined,
};

export class DynamoTable<
  Schema extends z.ZodObject<any, any, any>,
  Config extends RoughConfig<z.infer<Schema>>,
> {
  constructor(
    private readonly client: DynamoDBDocument,
    private readonly schema: Schema,
    private readonly config: Config,
  ) {}

  private keyFromRecord(
    record: z.infer<Schema>,
  ): CompleteKeyForIndex<z.infer<Schema>, Config['keys']> {
    return _pick(
      record,
      this.config.keys.hash,
      // @ts-expect-error
      this.config.keys.range,
    );
  }

  /**
   * Creates a new item in the table.
   *
   * @param record The item to create.
   * @param options Options for the put.
   * @returns The newly updated item.
   */
  async put(record: z.infer<Schema>, options?: PutOptions<z.infer<Schema>>) {
    const conditions: DynamoDBCondition<z.infer<Schema>>[] = [];
    if (!options?.overwrite) {
      conditions.push({
        'attribute-not-exists': [this.config.keys.hash],
      });
    }
    if (options?.condition) {
      conditions.push(options.condition);
    }
    await this.client.put({
      ...serializeCondition({ and: conditions }),
      TableName: this.config.tableName,
      Item: this.schema.parse(record),
    });
    return record;
  }

  /**
   * Fetches an item by its key.
   *
   * @param key The key of the record to fetch.
   * @param options Options for the fetch.
   *
   * @returns The fetched item, or `undefined` if the item does not exist.
   */
  async get(
    key: Required<CompleteKeyForIndex<z.infer<Schema>, Config['keys']>>,
    options?: GetOptions,
  ): Promise<z.infer<Schema> | undefined> {
    const result = await this.client.get({
      TableName: this.config.tableName,
      Key: key,
      ConsistentRead: options?.consistentRead,
    });
    if (!result.Item) {
      return undefined;
    }
    return this.schema.parse(result.Item);
  }

  /**
   * Deletes an item by its key.
   *
   * @param key
   * @param options
   */
  async delete(
    key: Required<CompleteKeyForIndex<z.infer<Schema>, Config['keys']>>,
    options?: DeleteOptions<z.infer<Schema>>,
  ) {
    await this.client.delete({
      ...(options?.condition ? serializeCondition(options.condition) : {}),
      TableName: this.config.tableName,
      Key: key,
    });
  }

  private async _query(params: {
    index?: string;
    key: KeyCondition<z.infer<Schema>, any>;
    options?: QueryOptions;
  }): Promise<QueryResponse<z.infer<Schema>>> {
    const keySchema = params.index
      ? this.config.secondaryIndexes[params.index]
      : this.config.keys;

    const options: QueryOptions = params.options ?? {};

    const result = await this.client.query({
      // @ts-expect-error
      ...serializeKeyCondition(keySchema, params.key),
      TableName: this.config.tableName,
      ...(params.index ? { IndexName: params.index } : {}),
      Limit: options.limit,
      ScanIndexForward: options.scanIndexForward,
      ExclusiveStartKey: PageToken.decode(options.nextPageToken),
      ConsistentRead: options.consistentRead,
    });

    return {
      items: (result.Items ?? []).map((item) => this.schema.parse(item)),
      nextPageToken: PageToken.encode(result.LastEvaluatedKey),
    };
  }

  /**
   * Performs a query against the table using the provided options.
   */
  async query(
    key: KeyCondition<z.infer<Schema>, Config['keys']>,
    options?: QueryOptions,
  ): Promise<QueryResponse<z.infer<Schema>>> {
    return this._query({ key, options });
  }

  /**
   * Performs a query against an index using the provided options.
   */
  async queryIndex<IndexName extends keyof Config['secondaryIndexes'] & string>(
    index: IndexName,
    key: KeyCondition<z.infer<Schema>, Config['secondaryIndexes'][IndexName]>,
    options?: QueryOptions,
  ): Promise<QueryResponse<z.infer<Schema>>> {
    return this._query({ index, key, options });
  }

  /**
   * Puts provided items using BatchWrite.
   *
   * It is safe to pass as many items as you want -- `batchDelete` will
   * automatically split the request into multiple batches if necessary,
   * and retry any failures.
   *
   * @param keys A list of keys.
   */
  async batchPut(records: z.infer<Schema>[]) {
    await batchWrite<'put'>({
      client: this.client,
      table: this.config.tableName,
      request: records.map((record) => ({
        PutRequest: { Item: this.schema.parse(record) },
      })),
    });
  }

  /**
   * Deletes the items matching the provided keys using BatchWrite.
   *
   * It is safe to pass as many keys as you want -- `batchDelete` will
   * automatically split the request into multiple batches if necessary,
   * and retry any failures.
   *
   * @param keys A list of keys.
   */
  async batchDelete(
    keys: CompleteKeyForIndex<z.infer<Schema>, Config['keys']>[],
  ) {
    await batchWrite<'delete'>({
      client: this.client,
      table: this.config.tableName,
      request: keys.map((key) => ({ DeleteRequest: { Key: key } })),
    });
  }

  private async _deleteAll(params: {
    index?: string;
    key: KeyCondition<z.infer<Schema>, any>;
    options?: DeleteAllOptions;
  }) {
    let nextPageToken: any = undefined;

    do {
      const result = await this._query({
        ...params,
        options: { ...params.options, limit: 500, nextPageToken },
      });

      nextPageToken = result.nextPageToken;

      await this.batchDelete(
        result.items.map((item) => this.keyFromRecord(item)),
      );
    } while (nextPageToken);
  }

  /**
   * Deletes all items that match the specified query.
   */
  async deleteAll(
    key: KeyCondition<z.infer<Schema>, Config['keys']>,
    options?: DeleteAllOptions,
  ) {
    return this._deleteAll({ key, options });
  }

  /**
   * Deletes all items that match the specified index query.
   */
  async deleteAllByIndex<
    IndexName extends keyof Config['secondaryIndexes'] & string,
  >(
    index: IndexName,
    key: KeyCondition<z.infer<Schema>, Config['secondaryIndexes'][IndexName]>,
    options?: Omit<QueryOptions, 'limit' | 'nextPageToken'>,
  ) {
    return this._deleteAll({ index, key, options });
  }

  /**
   * Applies a "patch" to an existing item.
   *
   * @param key The key of the item to patch.
   * @param patch The patch to apply.
   * @param options Options for the write.
   *
   * @returns The updated item.
   */
  async patch(
    key: Required<CompleteKeyForIndex<z.infer<Schema>, Config['keys']>>,
    patch: DynamoDBUpdate<z.infer<Schema>>,
    options?: PatchOptions<z.infer<Schema>>,
  ): Promise<z.infer<Schema>> {
    const result = await this.client.update({
      ...serializeUpdate({
        update: patch,
        condition: {
          and: [
            // Add a condition that object exists -- patch(...) should
            // not create records.
            { 'attribute-exists': [this.config.keys.hash] },
            options?.condition ?? {},
          ],
        },
      }),
      TableName: this.config.tableName,
      Key: key,
      ReturnValues: 'ALL_NEW',
    });

    return this.schema.parse(result.Attributes);
  }

  /**
   * Performs a guaranteed "strict" update on the specified item. Guarantees that the
   * modification described by the `calculate` function will be applied exactly. If the
   * state of the existing item changes in any way between the time it is fetched and the
   * time the update is applied, the update will fail.
   *
   * @param key The key of the item to update.
   * @param modification A function that takes the existing item (if it exists) and returns
   * the desired new state of the item.
   */
  async upsert(
    key: Required<CompleteKeyForIndex<z.infer<Schema>, Config['keys']>>,
    modification: (
      existing: z.infer<Schema> | undefined,
      retry: (reason: string) => never,
    ) => z.infer<Schema>,
  ): Promise<z.infer<Schema>> {
    return retry(
      async (bail) => {
        class RetryableError extends Error {}
        try {
          const existing = await this.get(key, { consistentRead: true });

          const updated = this.schema.parse(
            modification(existing, (reason) => {
              throw new RetryableError(reason);
            }),
          );

          // Remove the hash key -- including it on the update will result in an error.
          delete updated[this.config.keys.hash];

          const result = await this.client.update({
            ...serializeUpdate({
              update: { set: updated },
              condition: existing
                ? // If the item exists, make a write-time check to ensure that:
                  // - none of the existing attributes have changed, AND
                  // - any attributes being _added_ by the modification were not
                  //   added already
                  {
                    equals: existing,
                    'attribute-not-exists': Object.keys(updated).filter(
                      (key) => !Object.keys(existing).includes(key),
                    ),
                  }
                : { 'attribute-not-exists': [this.config.keys.hash] },
            }),
            TableName: this.config.tableName,
            Key: key,
            ReturnValues: 'ALL_NEW',
          });

          return this.schema.parse(result.Attributes);
        } catch (err: any) {
          // Only retry condition failures + explicitly retryable errors.
          // Assume any other error is terminal.
          if (
            err instanceof ConditionalCheckFailedException ||
            err instanceof RetryableError
          ) {
            throw err;
          }
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
          return bail(err) as never;
        }
      },
      { retries: 3, minTimeout: 100 },
    );
  }
}
