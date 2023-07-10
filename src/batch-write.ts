import {
  DynamoDBDocument,
  BatchWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import {
  WriteRequest,
  PutRequest,
  DeleteRequest,
} from '@aws-sdk/client-dynamodb';
import pMap from 'p-map';
import chunk from 'lodash/chunk';

export type PageQuery = {
  pageSize: number;
  nextPageToken?: string | undefined;
};

// Only process 25 items at a time to avoid error:
// Too many items requested for the BatchWriteItem call
const batchMaxItemCount = 25;

type BatchMode = 'put' | 'delete';

type Operation<TMode extends BatchMode> = Omit<
  WriteRequest,
  'PutRequest' | 'DeleteRequest'
> &
  // limit to only using put _or_ delete requests, not both
  // so that at the type level we can enforce only processing
  // 25 requests at a time
  TMode extends 'put'
  ? {
      PutRequest?: Omit<PutRequest, 'Item'> & {
        Item: Record<string, NativeAttributeValue> | undefined;
      };
    }
  : {
      DeleteRequest?: Omit<DeleteRequest, 'Key'> & {
        Key: Record<string, NativeAttributeValue> | undefined;
      };
    };

type BatchWriteInput<TMode extends BatchMode> = Pick<
  RetryBatchWriteInput,
  'client'
> & {
  table: string;
  request: Operation<TMode>[];
};

export const batchWrite = async <T extends BatchMode>(
  input: BatchWriteInput<T>,
) => {
  await pMap(
    chunk(input.request, batchMaxItemCount),
    (requests) =>
      retryBatchWrite({
        ...input,
        request: {
          [input.table]: requests,
        },
      }),
    {
      concurrency: 2,
    },
  );
};

type RetryBatchWriteInput = {
  client: DynamoDBDocument;
  request: BatchWriteCommandInput['RequestItems'];
  attempt?: number;
};

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const retryBatchWrite = async ({
  client,
  request,
  attempt = 0,
}: RetryBatchWriteInput) => {
  const maxAttempts = 5;
  if (attempt >= maxAttempts) {
    throw new Error(
      `Batch write returned some unprocessed items after ${attempt} attempts`,
    );
  }

  const response = await client.batchWrite({ RequestItems: request });

  if (
    !response.UnprocessedItems ||
    !Object.keys(response.UnprocessedItems).length
  ) {
    return response;
  }

  // progressively increase back off time on each attempt
  const multiplier = 2 ** attempt / 2;
  await delay(100 * multiplier);

  await retryBatchWrite({
    client,
    request: response.UnprocessedItems,
    attempt: attempt + 1,
  });
};
