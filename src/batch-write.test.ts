import chunk from 'lodash/chunk';

import { batchWrite } from './batch-write';
import {
  UserSchema,
  testUserTableName,
  useDynamoDB,
} from './test/utils/dynamodb';

const dynamo = useDynamoDB();

describe('batchWrite', () => {
  beforeAll(() => {
    jest.useFakeTimers();
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  it('splits writes into 25 item chunks and processes 2 chunks at a time', async () => {
    const usersToWrite = Array.from({ length: 75 }, (_, i) => ({
      id: `item-${i}`,
      account: 'account-1',
      createdAt: new Date().toISOString(),
    }));
    const writeRequest = usersToWrite.map((user) => ({
      PutRequest: {
        Item: UserSchema.parse(user),
      },
    }));
    const chunks = chunk(writeRequest, 25);

    // Mock two parallel requests that take 300ms in total so we can
    // assert that only at most 2 chunks get processed at a time. The
    // 3 chunks should take ~500ms to process.
    const batchWriteSpy = jest
      .spyOn(dynamo.documentClient, 'batchWrite')
      // eslint-disable-next-line @typescript-eslint/no-misused-promises
      .mockImplementationOnce(() => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve({});
          }, 100);
        });
      })
      // eslint-disable-next-line @typescript-eslint/no-misused-promises
      .mockImplementationOnce(() => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve({});
          }, 200);
        });
      })
      // eslint-disable-next-line @typescript-eslint/no-misused-promises
      .mockImplementationOnce(() => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve({});
          }, 300);
        });
      });

    const inFlightRequest = batchWrite<'put'>({
      client: dynamo.documentClient,
      table: testUserTableName,
      request: writeRequest,
    });

    await jest.advanceTimersByTimeAsync(50);

    expect(batchWriteSpy).toHaveBeenCalledTimes(2);
    expect(batchWriteSpy).toHaveBeenCalledWith({
      RequestItems: { [testUserTableName]: chunks[0] },
    });
    expect(batchWriteSpy).toHaveBeenCalledWith({
      RequestItems: { [testUserTableName]: chunks[1] },
    });

    // 50ms more for the first chunk to complete and the third chunk to start
    // getting processed (concurrency is 2!)
    await jest.advanceTimersByTimeAsync(50);
    expect(batchWriteSpy).toHaveBeenCalledTimes(3);
    expect(batchWriteSpy).toHaveBeenCalledWith({
      RequestItems: { [testUserTableName]: chunks[2] },
    });

    // Make sure request completes.
    await jest.runAllTimersAsync();
    await inFlightRequest;
  });

  it('retries unprocessed items with a backoff', async () => {
    const usersToWrite = Array.from({ length: 25 }, (_, i) => ({
      id: `item-${i}`,
      account: 'account-1',
      createdAt: new Date().toISOString(),
    }));
    const writeRequest = usersToWrite.map((user) => ({
      PutRequest: {
        Item: UserSchema.parse(user),
      },
    }));

    // Mock two parallel requests that take 300ms in total so we can
    // assert that only at most 2 chunks get processed at a time. The
    // 3 chunks should take ~500ms to process.
    const batchWriteSpy = jest
      .spyOn(dynamo.documentClient, 'batchWrite')
      // @ts-expect-error brachWrite can return a promise.
      .mockResolvedValueOnce({
        UnprocessedItems: {
          [testUserTableName]: writeRequest.slice(15),
        },
      })
      // @ts-expect-error brachWrite can return a promise.
      .mockResolvedValueOnce({
        UnprocessedItems: {
          [testUserTableName]: writeRequest.slice(20),
        },
      })
      // @ts-expect-error brachWrite can return a promise.
      .mockResolvedValueOnce({});

    const inFlightRequest = batchWrite<'put'>({
      client: dynamo.documentClient,
      table: testUserTableName,
      request: writeRequest,
    });

    // Original call.
    await jest.advanceTimersByTimeAsync(1);
    expect(batchWriteSpy).toHaveBeenCalledTimes(1);
    expect(batchWriteSpy).toHaveBeenCalledWith({
      RequestItems: { [testUserTableName]: writeRequest },
    });

    // First retry after 50ms backoff.
    await jest.advanceTimersByTimeAsync(50);
    expect(batchWriteSpy).toHaveBeenCalledTimes(2);
    expect(batchWriteSpy).toHaveBeenCalledWith({
      RequestItems: { [testUserTableName]: writeRequest.slice(15) },
    });

    // Second retry after 100ms backoff.
    await jest.advanceTimersByTimeAsync(100);
    expect(batchWriteSpy).toHaveBeenCalledTimes(3);
    expect(batchWriteSpy).toHaveBeenCalledWith({
      RequestItems: { [testUserTableName]: writeRequest.slice(20) },
    });

    // Make sure request completes.
    await jest.runAllTimersAsync();
    await inFlightRequest;
  });

  it('throws an error if unprocessed items remain after 5 attempts', async () => {
    const usersToWrite = Array.from({ length: 25 }, (_, i) => ({
      id: `item-${i}`,
      account: 'account-1',
      createdAt: new Date().toISOString(),
    }));
    const writeRequest = usersToWrite.map((user) => ({
      PutRequest: {
        Item: UserSchema.parse(user),
      },
    }));

    const batchWriteSpy = jest
      .spyOn(dynamo.documentClient, 'batchWrite')
      // @ts-expect-error brachWrite can return a promise.
      .mockResolvedValue({
        UnprocessedItems: {
          [testUserTableName]: writeRequest.slice(15),
        },
      });

    const inFlightRequest = batchWrite<'put'>({
      client: dynamo.documentClient,
      table: testUserTableName,
      request: writeRequest,
    });

    void jest.runAllTimersAsync();

    await expect(inFlightRequest).rejects.toThrow(
      'Batch write returned some unprocessed items after 5 attempts',
    );

    expect(batchWriteSpy).toHaveBeenCalledTimes(5);
  });
});
