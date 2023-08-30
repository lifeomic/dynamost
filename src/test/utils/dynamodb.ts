import { CreateTableInput, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { dynamoDBTestHooks } from '@lifeomic/test-tool-dynamodb';
import { z } from 'zod';

export const testUserTableName = 'dynamost-user-table';

const SCHEMAS: CreateTableInput[] = [
  {
    TableName: testUserTableName,
    KeySchema: [{ KeyType: 'HASH', AttributeName: 'id' }],
    GlobalSecondaryIndexes: [
      {
        IndexName: 'account-index',
        KeySchema: [
          {
            KeyType: 'HASH',
            AttributeName: 'account',
          },
          { KeyType: 'RANGE', AttributeName: 'createdAt' },
        ],
        Projection: { ProjectionType: 'ALL' },
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
      },
    ],
    AttributeDefinitions: [
      { AttributeName: 'id', AttributeType: 'S' },
      { AttributeName: 'account', AttributeType: 'S' },
      { AttributeName: 'createdAt', AttributeType: 'S' },
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  },
];

export type UseDynamoDBContext = {
  dynamoDBClient: DynamoDBClient;
  documentClient: DynamoDBDocument;
};

export const useDynamoDB = () => {
  const internalContext: any = {};
  const context: UseDynamoDBContext = {} as any;
  const hooks = dynamoDBTestHooks({ schemas: SCHEMAS, useLocalDynamoDb: true });

  beforeAll(hooks.beforeAll, 100000);
  beforeEach(async () => {
    const ctx = await hooks.beforeEach();
    Object.assign(internalContext, ctx);
    context.dynamoDBClient = ctx.dynamoDBClient;
    context.documentClient = DynamoDBDocument.from(ctx.dynamoDBClient);
  }, 100000);

  afterAll(hooks.afterAll, 100000);
  afterEach(async () => {
    if (internalContext.dynamoDBClient) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      await hooks.afterEach(internalContext);
    }
  }, 100000);

  return context;
};

export const UserSchema = z.object({
  createdAt: z.string().datetime(),
  account: z.string(),
  id: z.string(),
});

export const UserTableDefinition = {
  tableName: testUserTableName,
  keys: { hash: 'id', range: 'createdAt' },
  secondaryIndexes: {
    'account-index': { hash: 'account', range: 'createdAt' },
  },
} as const;
