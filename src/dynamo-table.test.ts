import { z } from 'zod';

import { DynamoTable } from './dynamo-table';
import { testUserTableName, useDynamoDB } from './test/utils/dynamodb';
import { TransactionManager } from './transaction-manager';

const dynamo = useDynamoDB();
const transactionManager = new TransactionManager(dynamo.documentClient);

export const UserSchema = z.object({
  createdAt: z.string().datetime(),
  account: z.string(),
  id: z.string(),
});

const userTable = new DynamoTable(dynamo.documentClient, UserSchema, {
  tableName: testUserTableName,
  keys: { hash: 'id', range: 'createdAt' },
  secondaryIndexes: {
    'account-index': { hash: 'account', range: 'createdAt' },
  },
});

describe('DynamoTable', () => {
  it('works with patch', async () => {
    await transactionManager.run((transaction) => {
      const x = userTable.patch(
        '1',
        {
          set: {
            account: 'test',
          },
        },
        { transaction },
      );
    });
  });
});
