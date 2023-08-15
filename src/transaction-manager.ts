import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

type WriteTransactionItem = NonNullable<
  Parameters<DynamoDBDocument['transactWrite']>[0]['TransactItems']
>[0];

export interface Transaction {
  addWrite(writeItem: WriteTransactionItem): void;
}

/**
 * The callback that TransactionManager.run takes. Callers should pass in a
 * function that takes a transaction object and then does all the writes needed
 * by calling relevant DynamoTable methods which accept said object. Afterward,
 * the writes will be made in a single transaction.
 *
 * The function may also return a value, which will be the return value of
 * "TransactionManager.run".
 */
type TransactionRun<T> = (transaction: Transaction) => Promise<T>;

export class TransactionManager {
  private writes: WriteTransactionItem[] = [];
  private transaction: Transaction = {
    addWrite: (item: WriteTransactionItem) => {
      this.writes.push(item);
    },
  };

  constructor(private readonly client: DynamoDBDocument) {}

  async run<T>(transactionRun: TransactionRun<T>): Promise<T> {
    const result = await transactionRun(this.transaction);

    if (this.writes.length > 0) {
      await this.client.transactWrite({ TransactItems: this.writes });
    }

    return result;
  }
}
