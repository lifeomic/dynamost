import {
  UserSchema,
  UserTableDefinition,
  useDynamoDB,
} from './test/utils/dynamodb';
import { DynamoTable } from './';
import { TransactionManager } from './transaction-manager';

const dynamo = useDynamoDB();

describe('DynamoTable', () => {
  const setupDb = () => {
    const userTable = new DynamoTable(
      dynamo.documentClient,
      UserSchema,
      UserTableDefinition,
    );
    const transactionManager = new TransactionManager(dynamo.documentClient);

    return {
      userTable,
      transactionManager,
    };
  };

  describe('put/get/delete', () => {
    it('can write, read, and delete a record', async () => {
      const { userTable } = setupDb();

      const newUser = {
        id: 'new-user',
        account: 'account-1',
        createdAt: new Date().toISOString(),
      };
      await userTable.put(newUser);

      const fetchedUser = await userTable.get({ id: newUser.id });

      expect(fetchedUser?.id).toBe(newUser.id);

      await userTable.delete({ id: newUser.id });

      const deletedUser = await userTable.get({ id: newUser.id });
      expect(deletedUser).toBeUndefined();
    });

    it('can conditionally patch/delete a record', async () => {
      const { userTable } = setupDb();

      const user1 = {
        id: 'user-1',
        account: 'account-1',
        createdAt: new Date().toISOString(),
      };

      await userTable.put(user1);

      await expect(async () => {
        await userTable.patch(
          { id: user1.id },
          {
            set: {
              account: 'account-3',
            },
          },
          {
            condition: { equals: { account: 'account-2' } },
          },
        );
      }).rejects.toThrow('The conditional request failed');

      await userTable.patch(
        { id: user1.id },
        {
          set: {
            account: 'account-2',
          },
        },
        {
          condition: { equals: { account: 'account-1' } },
        },
      );

      const fetchedUser = await userTable.get({ id: user1.id });
      expect(fetchedUser?.account).toBe('account-2');

      await expect(async () => {
        await userTable.delete(
          { id: user1.id },
          {
            condition: { equals: { account: 'account-1' } },
          },
        );
      }).rejects.toThrow('The conditional request failed');

      await userTable.delete(
        { id: user1.id },
        {
          condition: { equals: { account: 'account-2' } },
        },
      );

      const fetchedUser2 = await userTable.get({ id: user1.id });
      expect(fetchedUser2).toBeUndefined();
    });

    it('can use put to override existing record', async () => {
      const { userTable } = setupDb();

      const user1 = {
        id: 'user-1',
        account: 'account-1',
        createdAt: new Date().toISOString(),
      };

      await userTable.put(user1);

      await expect(async () => {
        await userTable.put(user1);
      }).rejects.toThrow('The conditional request failed');
    });
  });

  describe('batchGet', () => {
    it('can fetch multiple items through batch api', async () => {
      const batchGetSpy = jest.spyOn(dynamo.documentClient, 'batchGet');
      const { userTable } = setupDb();

      const ids = Array.from({ length: 101 }, (_, index) => String(index));

      // seed the db
      await userTable.batchPut(
        ids.map((id) => ({
          id,
          account: 'account-1',
          createdAt: new Date().toISOString(),
        })),
      );

      const [first] = ids;

      const batchGetFirst = await userTable.batchGet([{ id: first }], {
        consistentRead: true,
      });

      expect(batchGetFirst).toStrictEqual([
        { id: first, account: 'account-1', createdAt: expect.any(String) },
      ]);
      expect(batchGetSpy).toHaveBeenCalledTimes(1);
      expect(batchGetSpy).toHaveBeenCalledWith({
        RequestItems: {
          'dynamost-user-table': {
            ConsistentRead: true,
            Keys: [
              {
                id: '0',
              },
            ],
          },
        },
      });

      batchGetSpy.mockClear();
      const batchGetUnknown = await userTable.batchGet([{ id: 'unknown' }]);

      expect(batchGetSpy).toHaveBeenCalledTimes(1);
      expect(batchGetSpy).toHaveBeenCalledWith({
        RequestItems: {
          'dynamost-user-table': {
            ConsistentRead: undefined,
            Keys: [
              {
                id: 'unknown',
              },
            ],
          },
        },
      });

      expect(batchGetUnknown).toStrictEqual([]);

      batchGetSpy.mockClear();
      const batchGetOverLimit = await userTable.batchGet(
        ids.map((id) => ({ id })),
      );

      expect(batchGetOverLimit.length).toBeGreaterThan(100);
      expect(batchGetOverLimit).toHaveLength(ids.length);
      expect(batchGetSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe('upsert', () => {
    // TODO
  });

  describe('query', () => {
    it('can query a table with pagination', () => {
      // TODO
    });
  });

  describe('queryIndex', () => {
    it('can query a table index with pagination', () => {
      // TODO
    });
  });

  describe('deleteAll', () => {
    it('deletes all the records that match the query', async () => {
      // TODO
    });
  });

  describe('deleteAllByIndex', () => {
    it('deletes all the records that match the index query', async () => {
      // TODO
    });
  });

  describe('transactions', () => {
    it('can execute transactional writes', async () => {
      const { userTable, transactionManager } = setupDb();
      expect.assertions(9);

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

      // Test that a transaction with an unmet condition does not make any
      // changes.
      await expect(async () => {
        await transactionManager.run((transaction) => {
          // The first two actions should not go through because the condition
          // check will fail.
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
          userTable.conditionTransact(
            { id: 'user-1' },
            {
              // This condition causes a failure, since user 1 is assigned to
              // account 2.
              condition: { equals: { account: 'account-1' } },
              transaction,
            },
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

      // One more time, validate that the first user is still present, and that
      // the second user still has the same account.
      expect(user1?.account).toBe('account-2');
      expect(user2?.account).toBe('account-1');
    });

    it('throws if an empty transaction is made', async () => {
      const transactionManager = new TransactionManager(dynamo.documentClient);

      await expect(async () => {
        await transactionManager.run(() => {
          // no added writes - this should throw!
        });
      }).rejects.toThrow('No writes were added to the transaction');
    });
  });
});
