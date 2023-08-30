import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoTable, TableKey, TransactionItem } from './dynamo-table';
import { z } from 'zod';

type Transaction<Tables> = {
  [Key in keyof Tables]?: Tables[Key] extends DynamoTable<infer Schema, any>
    ? TransactionItem<z.infer<Schema>, Required<TableKey<Tables[Key]>>>[]
    : never;
};

export class Transacter<
  Tables extends { [key: string]: DynamoTable<any, any> },
> {
  constructor(
    private readonly client: DynamoDBDocument,
    private readonly tables: Tables,
  ) {}

  async transactWrite(txn: Transaction<Tables>): Promise<void> {
    const items = Object.entries(txn)
      .map(([table, items]) =>
        this.tables[table].toTransactWriteItems(items ?? []),
      )
      .flat();

    await this.client.transactWrite({ TransactItems: items });
  }
}
