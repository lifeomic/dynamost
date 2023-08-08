import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { v4 as uuid } from 'uuid';

type WriteTransactionItem = NonNullable<
  Parameters<DynamoDBDocument['transactWrite']>[0]['TransactItems']
>[0];

type WriteTransaction = {
  id: string;
  items: WriteTransactionItem[];
};

export class TransactionManager {
  private writeTransactions: Record<string, WriteTransaction> = {};

  constructor(private readonly client: DynamoDBDocument) {}

  createWrite() {
    const transactionId = uuid();

    this.writeTransactions[transactionId] = {
      id: transactionId,
      items: [],
    };

    const transactionInstance = {
      add: (item: WriteTransactionItem) => {
        this.writeTransactions[transactionId].items.push(item);

        return transactionInstance;
      },
      commit: async () => {
        const transaction = this.writeTransactions[transactionId];

        await this.client.transactWrite({
          TransactItems: transaction.items,
        });

        delete this.writeTransactions[transactionId];
      },
    };

    return;
  }

  // TODO: support read transactions.
}
