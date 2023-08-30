import { z } from 'zod';

import { testUserTableName, useDynamoDB } from './test/utils/dynamodb';
import { DynamoTable } from './dynamo-table';
import { TransactionManager } from './transaction-manager';

const dynamo = useDynamoDB();

describe('DynamoTable', () => {
  const UserSchema = z.object({
    createdAt: z.string().datetime(),
    account: z.string(),
    id: z.string(),
  });

  it('can execute transactional writes', async () => {
    const userTable = new DynamoTable(dynamo.documentClient, UserSchema, {
      tableName: testUserTableName,
      keys: { hash: 'id', range: 'createdAt' },
      secondaryIndexes: {
        'account-index': { hash: 'account', range: 'createdAt' },
      },
    });
    const transactionManager = new TransactionManager(dynamo.documentClient);

    expect.assertions(6);

    const user = await userTable.put({
      id: 'user-1',
      account: 'account-1',
      createdAt: new Date().toISOString(),
    });

    const fetchedUser = await userTable.get({
      id: user.id,
    });

    expect(fetchedUser?.id).toBe(user.id);

    await transactionManager.run((transaction) => {
      userTable.putTransact(
        {
          id: 'user-2',
          account: 'account-1',
          createdAt: new Date().toISOString(),
        },
        { transaction },
      );

      userTable.patchTransact(
        { id: user.id },
        {
          set: {
            account: 'account-2',
          },
        },
        { transaction },
      );
    });

    let [user1, user2] = await Promise.all([
      userTable.get({ id: 'user-1' }),
      userTable.get({ id: 'user-2' }),
    ]);

    expect(user1?.account).toBe('account-2');
    expect(user2?.account).toBe('account-1');

    // Test that a failed transaction does not make any changes.
    await expect(async () => {
      await transactionManager.run((transaction) => {
        // The first two actions should not go through because the patch on a
        // non existent user will fail.
        userTable.deleteTransact({ id: 'user-1' }, { transaction });

        userTable.patchTransact(
          { id: 'user-2' },
          {
            set: {
              account: 'account-3',
            },
          },
          { transaction },
        );

        // This is the operation that causes the transaction to fail.
        userTable.patchTransact(
          { id: 'non-existent-user' },
          {
            set: {
              account: 'account-2',
            },
          },
          { transaction },
        );
      });
    }).rejects.toThrow(
      new Error(
        'Transaction cancelled, please refer cancellation reasons for specific reasons [None, None, ConditionalCheckFailed]',
      ),
    );

    [user1, user2] = await Promise.all([
      userTable.get({ id: 'user-1' }),
      userTable.get({ id: 'user-2' }),
    ]);

    // Validate that the first user is still present, and that the second user
    // still has the same account.
    expect(user1?.account).toBe('account-2');
    expect(user2?.account).toBe('account-1');
  });
});
