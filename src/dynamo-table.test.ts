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

  describe('scan', () => {
    it('can scan a table with pagination', async () => {
      const { userTable } = setupDb();

      // seed the db
      await userTable.batchPut([
        {
          id: 'user-1',
          account: 'account-1',
          createdAt: new Date().toISOString(),
        },
        {
          id: 'user-2',
          account: 'account-1',
          createdAt: new Date().toISOString(),
        },
        {
          id: 'user-3',
          account: 'account-2',
          createdAt: new Date().toISOString(),
        },
      ]);

      const firstSet = await userTable.scan({
        limit: 2,
      });

      expect(firstSet.items).toHaveLength(2);
      expect(firstSet.nextPageToken).toBeDefined();

      const secondSet = await userTable.scan({
        limit: 2,
        nextPageToken: firstSet.nextPageToken,
      });

      expect(secondSet.items).toHaveLength(1);
      expect(secondSet.nextPageToken).not.toBeDefined();

      // pagination when filtering
      const filteredSet = await userTable.scan({
        limit: 1,
        filter: { 'less-than': { id: 'user-3' } },
      });

      expect(filteredSet.items).toHaveLength(1);
      expect(filteredSet.nextPageToken).toBeDefined();

      const fullSet = await userTable.scan();

      expect(fullSet.items).toHaveLength(3);
      expect(fullSet.nextPageToken).not.toBeDefined();
    });

    it('can scan the table with a filter', async () => {
      const { userTable } = setupDb();

      // seed the db
      await userTable.batchPut([
        {
          id: 'user-1',
          account: 'account-1',
          createdAt: '2018-01-01T23:00:00.000Z',
        },
        {
          id: 'user-2',
          account: 'account-1',
          createdAt: '2019-01-01T23:00:00.000Z',
        },
        {
          id: 'user-3',
          account: 'account-2',
          createdAt: '2020-01-01T23:00:00.000Z',
        },
      ]);

      const accountOne = await userTable.scan({
        filter: { equals: { account: 'account-1' } },
      });

      expect(accountOne.items).toHaveLength(2);
      expect(accountOne.items).toStrictEqual([
        {
          id: 'user-1',
          account: 'account-1',
          createdAt: '2018-01-01T23:00:00.000Z',
        },
        {
          id: 'user-2',
          account: 'account-1',
          createdAt: '2019-01-01T23:00:00.000Z',
        },
      ]);

      const notAccountOne = await userTable.scan({
        filter: { 'not-equals': { account: 'account-1' } },
      });

      expect(notAccountOne.items).toHaveLength(1);
      expect(notAccountOne.items).toStrictEqual([
        {
          id: 'user-3',
          account: 'account-2',
          createdAt: '2020-01-01T23:00:00.000Z',
        },
      ]);

      const usersCreatedSince = await userTable.scan({
        filter: { 'greater-than': { createdAt: '2020-01-01' } },
      });

      expect(usersCreatedSince.items).toHaveLength(1);
      expect(usersCreatedSince.items).toStrictEqual([
        {
          id: 'user-3',
          account: 'account-2',
          createdAt: '2020-01-01T23:00:00.000Z',
        },
      ]);

      const usersByMultipleConditions = await userTable.scan({
        filter: {
          'greater-than': { id: 'user-1' },
          'less-than': { id: 'user-3' },
        },
      });

      expect(usersByMultipleConditions.items).toHaveLength(1);
      expect(usersByMultipleConditions.items).toStrictEqual([
        {
          id: 'user-2',
          account: 'account-1',
          createdAt: '2019-01-01T23:00:00.000Z',
        },
      ]);
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
