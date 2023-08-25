This package provides helpful TypeScript utilities for interacting with a
DynamoDB table and maintaining complete type safety.

## Installation

```bash
yarn add @lifeomic/dynamost @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

## Usage

First, declare a _schema_ for your table using
[`zod`](https://github.com/colinhacks/zod). This schema can be arbitarily
complex, but it must reflect a JSON-serializable object.

```typescript
const MySchema = z.object({
  id: z.string(),
  name: z.string(),
  createdAt: z.string().datetime(),
  deletable: z.boolean().optional(),
});
```

Next, create a `DynamoTable` instance using the schema and the configuration of
the table in DynamoDB.

```typescript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { DynamoTable } from '@lifeomic/dynamost';

const table = new DynamoTable(
  // Provide a Document client.
  DynamoDBDocument.from(new DynamoDBClient({})),
  // Specify your Schema.
  MySchema,
  // Specify the table configuration.
  {
    // Specify the table name,
    tableName: 'my-table',
    // Specify the key schema for the table.
    keys: { hash: 'id', range: undefined },
    // Specify any secondary indexes.
    secondaryIndexes: {
      'name-index': { hash: 'name', range: 'createdAt' },
    },
  },
);
```

Now, you can begin interacting with the table.

```typescript
await table.put({
  id: '123',
  name: 'First Item',
  createdAt: new Date().toISOString(),
});

await table.get({ id: '123' });

await table.queryIndex('name-index', { id: '123' });
```

For details on the available methods, see the [API reference](#api-reference).

## Conditions and Expressions

Wherever conditions or expressions are supported in the API, Dynamost uses a
custom expression syntax that allows for type safety and maximizes readability.

For more details, see:

- [Update Expression Syntax](#update-expression-syntax)
- [Condition Expression Syntax](#condition-expression-syntax)

## API Reference

### `DynamoTable`

#### `put`

Creates an item in the table.

```typescript
const result = await table.put({
  id: '123',
  name: 'First Item',
  createdAt: new Date().toISOString(),
});
```

By default, `put` will _not_ overwrite existing items, and will throw a
`ConditionCheckFailedException` if the specified item already exists in the
table. If you want to overwrite existing items, you can use the `overwrite`
option:

```typescript
const result = await table.put(
  {
    id: '123',
    name: 'First Item',
    createdAt: new Date().toISOString(),
  },
  { overwrite: true },
);
```

`put` also accepts an optional `condition` parameter that can be used to assert
a condition:

```typescript
const result = await table.put(
  {
    id: '123',
    name: 'First Item',
    createdAt: new Date().toISOString(),
  },
  {
    overwrite: true,
    condition: {
      equals: { name: 'First Item' },
    },
  },
);
```

#### `get`

Retrieves an item in the table by its key. Returns `undefined` if the item does
not exist.

```typescript
const result = await table.get({ id: '123' });
```

To perform a consistent read, use the `consistentRead` option:

```typescript
const result = await table.get({ id: '123' }, { consistentRead: true });
```

#### `delete`

Deletes an item in the table by its key.

```typescript
await table.delete({ id: '123' });
```

`delete` is idempotent by default, and will not throw an error if the item does
not exist.

`delete` accepts an optional `condition` parameter that can be used to assert a
condition:

```typescript
await table.delete(
  { id: '123' },
  {
    condition: {
      equals: { name: 'First Item' },
    },
  },
);
```

#### `query`

Performs a query against the table. To query an index, use
[`queryIndex`](#queryIndex)

```typescript
const result = await table.query({ id: '123' });

// The list of items returned by the query.
result.items;
// An opaque token that can be used to retrieve the next page of results.
result.nextPageToken;
```

If the table is configured with a range key, you can specify a key condition for
the range key:

```typescript
await table.query({
  id: '123',
  createdAt: {
    'greater-than': new Date().toISOString(),
  },
});
```

`query` accepts a number of options:

```typescript
const result = await table.query(
  { id: '123' },
  {
    /** The maximum number of records to retrieve. */
    limit: 100,
    /** Whether to scan the index in ascending order. Defaults to `true`. */
    scanIndexForward: false,
    /**
     * A page token from a previous query. If provided, the query will
     * resume from where the previous query left off.
     */
    nextPageToken: '...',
    /**
     * Whether to perform a consistent query. Only valid when querying
     * the main table.
     */
    consistentRead: true,
  },
);
```

#### `queryIndex`

Performs a query against a secondary index.

```typescript
const result = await table.queryIndex('name-index', { name: 'First Item' });

// The list of items returned by the query.
result.items;
// An opaque token that can be used to retrieve the next page of results.
result.nextPageToken;
```

`queryIndex` accepts the same options as [`query`](#query).

#### `patch`

Applies a "patch" to a single record in the table.

For more details on the syntax of patches, see
[Update Expression Syntax](#update-expression-syntax).

```typescript
const updated = await table.patch(
  // The key of the item to patch.
  { id: '123' },
  // An update expression.
  { set: { name: 'Updated Item' } },
);
```

If the item does not exist, `patch` will throw a
`ConditionCheckFailedException`.

`patch` also accepts an optional `condition` parameter that can be used to
assert a condition:

```typescript
const result = await table.patch(
  { id: '123' },
  { set: { name: 'Updated Item' } },
  {
    condition: {
      equals: { name: 'First Item' },
    },
  },
);
```

#### `upsert`

Modifies (or creates) an item using
[optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control).

```typescript
const updated = await table.upsert(
  // The key of the item to patch.
  { id: '123' },
  // A modification function that returns the desired new state of the item.
  (existing) => {
    if (!existing) {
      throw new Error('Item does not exist');
    }
    return { ...existing, name: 'Updated Item' };
  },
);
```

**"Locking" Strategy**

`upsert` implements an "optimistic lock" against the _entire_ item. So:

If the modification function is called with `undefined`, then:

- The item did not exist at read-time, and
- The returned "new state" of the item will _only_ be applied if the item
  _still_ does not exist at write-time.

Otherwise, if the `modification` function is called with an existing item, then:

- The item did exist and read-time, and
- The returned "new state" of the item will _only_ be applied if _all_ of
  existing item's attributes are the same at write-time.

**Important**

- The locking strategy does **not** prevent writes in the case of a _new
  attribute_ being added to an item during the course of a modification.

- `upsert` will automatically retry if it encounters a condition check failure.
  Retries will re-fetch the existing item, and re-run the modification function.
  If the maximum number of retries is exceeded, `upsert` will re-throw the final
  `ConditionCheckFailedException`.

- By default, any errors thrown during the modification function will _not_
  trigger retries, and will be immediately re-thrown by `upsert`. In order to
  throw an error that will trigger retries, use the `retry` function:

  ```typescript
  await table.upsert({ id: '123' }, (existing, retry) => {
    if (existing.name !== 'First Item') {
      return retry('Item does not have expected name yet');
    }
    return { ...existing, name: 'Updated Item' };
  });
  ```

### `batchPut`

Puts multiple items to the table.

```typescript
await table.batchPut([
  {
    id: '123',
    name: 'First Item',
    createdAt: new Date().toISOString(),
  },
  {
    id: '124',
    name: 'Second Item',
    createdAt: new Date().toISOString(),
  },
  {
    id: '125',
    name: 'Third Item',
    createdAt: new Date().toISOString(),
  },
]);
```

### `batchDelete`

Deletes the specified items from the table.

```typescript
await table.batchDelete([{ id: '123' }, { id: '124' }, { id: '125' }]);
```

### `deleteAll`

Deletes all items that match the specified query. Generally accepts the same
parameters as [`query`](#query).

To delete against an index, use [`deleteAllForIndex`](#deleteAllForIndex)

```typescript
await table.deleteAll({ id: '123' });
```

### `deleteAllForIndex`

Deletes all items that match the specified index query. Generally accepts the
same parameters as [`queryIndex`](#queryIndex).

```typescript
await table.deleteAllForIndex('name-index', { name: 'First Item' });
```

## Update Expression Syntax

### `set`

```typescript
// Sets the "name" attribute to "Updated Item"
const update = {
  set: {
    name: 'Updated Item',
  },
};
// Sets the "name" attribute to "Updated Item"
// AND
// Sets the "deletable" attribute to `true`
const update = {
  set: {
    name: 'Updated Item',
    deletable: true,
  },
};
```

<!-- TODO: document "remove" if/when support is added. -->

## Condition Expression Syntax

### Condition Expression Operators

The following expression operators are supported:

#### `attribute-exists`

```typescript
// Asserts that the item has a "deletable" attribute.
const condition = {
  'attribute-exists': ['deletable'],
};
```

#### `attribute-not-exists`

```typescript
// Asserts that the item does _not_ have a "deletable" attribute.
const condition = {
  'attribute-not-exists': ['deletable'],
};
```

#### `equals`

```typescript
// Asserts that the item has a "name" attribute with the value "First Item".
const condition = {
  equals: {
    name: 'First Item',
  },
};
```

#### `not-equals`

```typescript
// Asserts that the item does not have a "name" attribute with the value "First Item".
const condition = {
  'not-equals': {
    name: 'First Item',
  },
};
```

#### `between`

```typescript
// Asserts that the item's "createdAt" value is between the two values.
const condition = {
  between: {
    createdAt: [
      new Date('2020-01-01').toISOString(),
      new Date('2020-01-02').toISOString(),
    ],
  },
};
```

#### `begins-with`

```typescript
// Asserts that the item's "createdAt" value begins with "2020-01-01".
const condition = {
  'begins-with': {
    createdAt: '2020-01-01',
  },
};
```

#### `greater-than`

```typescript
// Asserts that the item's "createdAt" value is greater than "2020-01-01".
const condition = {
  'greater-than': {
    createdAt: '2020-01-01',
  },
};
```

#### `greater-than-or-equal-to`

```typescript
// Asserts that the item's "createdAt" value is greater than or equal to "2020-01-01".
const condition = {
  'greater-than-or-equal-to': {
    createdAt: '2020-01-01',
  },
};
```

#### `less-than`

```typescript
// Asserts that the item's "createdAt" value is less than "2020-01-01".
const condition = {
  'less-than': {
    createdAt: '2020-01-01',
  },
};
```

#### `less-than-or-equal-to`

```typescript
// Asserts that the item's "createdAt" value is less than or equal to "2020-01-01".
const condition = {
  'less-than-or-equal-to': {
    createdAt: '2020-01-01',
  },
};
```

### Composing Conditions

Conditions can be composed in a handful of ways.

#### `and`

```typescript
// Asserts that:
// - the item has a "name" attribute with the value "First Item".
// AND
// - the item has a "deletable" attribute.
const condition = {
  and: [
    { equals: { name: 'First Item' } },
    { 'attribute-exists': ['deletable'] },
  ],
};
```

Using multiple entries in a single condition operator is equivalent to using
`and`.

```typescript
// Asserts that:
// - the item has a "name" attribute with the value "First Item".
// AND
// - the item has a "deletable" attribute with the value `true`
const condition = {
  equals: {
    name: 'First Item',
    deletable: true,
  },
};
```

#### `or`

```typescript
// Asserts that:
// - the item has a "name" attribute with the value "First Item".
// OR
// - the item has a "deletable" attribute.
const condition = {
  or: [
    { equals: { name: 'First Item' } },
    { 'attribute-exists': ['deletable'] },
  ],
};
```

## Transactions

Transactions are supported via the `TransactionManager` class, which is nicely
integrated with the methods that the `DynamoTable` exposes.

### Usage

```typescript
const client = new DynamoDBClient({});

const userTable = { /* some user table */ }
const membershipTable = { /* some membership table */ }
const transactionManager = new TransactionManager(client);

// Run any custom logic that requires a transaction inside the callback passed
// to "transactionManager.run". This was inspired by the sequelize transaction
// API.
await transactionManager.run(async (transaction) => {
  // Write any custom logic here. Leverage transactional writes by passing in
  // the transaction object to any of the DynamoTable methods that accept it.

  const newUser = await userTable.patch({
    name: 'John Doe',
  }, { transaction });

  // This won't actually commit the write at this point. It'll gather all writes
  // and execute all the callback's logic first, and then it will try to
  // commit all the write transactions at once.
  const result = await userTable.patch(
    { id: 'user-id' },
    { set: { name: 'John Doe The Second' } },
    {
      condition: {
        equals: { name: 'John Doe The First' },
      },
      transaction,
    },
  );

  // Some more custom logic, it can be anything...

  if (!process.env.PREMIUM_MEMBERSHIPS_ENABLED) {
    await membershipModel.delete({
      id: 'membership-id',
    }, { transaction })
  }
});
```

## Caveats

The `TransactionManager` currently only supports write transactions. Transaction support can progressively be added to each of the methods inside `DynamoTable`
by passing in an optional `Transaction` parameter.
