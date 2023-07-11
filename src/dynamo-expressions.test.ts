import {
  DynamoDBExpression,
  DynamoDBUpdate,
  serializeExpression,
  serializeUpdate,
} from './dynamo-expressions';

type TestEntity = {
  id: string;
  account: string;
  name: string;
};

describe('serializeExpression', () => {
  const CASES: {
    input: DynamoDBExpression<TestEntity>;
    expect: ReturnType<typeof serializeExpression>;
  }[] = [
    {
      input: {},
      expect: {},
    },
    {
      input: {
        'attribute-exists': [],
      },
      expect: {},
    },
    {
      input: {
        'attribute-not-exists': [],
      },
      expect: {},
    },
    {
      input: {
        'attribute-exists': ['id', 'account'],
      },
      expect: {
        ConditionExpression:
          'attribute_exists(#id) AND attribute_exists(#account)',
        ExpressionAttributeNames: {
          '#id': 'id',
          '#account': 'account',
        },
        ExpressionAttributeValues: undefined,
      },
    },
    {
      input: {
        or: [
          {
            'attribute-exists': ['id', 'account'],
          },
        ],
      },
      expect: {
        ConditionExpression:
          'attribute_exists(#id) AND attribute_exists(#account)',
        ExpressionAttributeNames: {
          '#id': 'id',
          '#account': 'account',
        },
        ExpressionAttributeValues: undefined,
      },
    },
    {
      input: {
        or: [
          { 'attribute-exists': ['id', 'account'] },
          { 'attribute-not-exists': ['name', 'id'] },
        ],
      },
      expect: {
        ConditionExpression:
          '(attribute_exists(#id) AND attribute_exists(#account)) OR (attribute_not_exists(#name) AND attribute_not_exists(#id))',
        ExpressionAttributeNames: {
          '#id': 'id',
          '#account': 'account',
          '#name': 'name',
        },
        ExpressionAttributeValues: undefined,
      },
    },
    {
      input: {
        equals: {
          id: '123',
        },
        'not-equals': {
          name: 'test-name',
        },
      },
      expect: {
        ConditionExpression: '#id = :ref0 AND #name <> :ref1',
        ExpressionAttributeNames: {
          '#id': 'id',
          '#name': 'name',
        },
        ExpressionAttributeValues: {
          ':ref0': '123',
          ':ref1': 'test-name',
        },
      },
    },
    {
      input: {
        or: [
          { 'attribute-not-exists': ['account'] },
          {
            and: [
              { equals: { id: '123' } },
              { 'not-equals': { name: 'test-name' } },
            ],
          },
        ],
      },
      expect: {
        ConditionExpression:
          '(attribute_not_exists(#account)) OR ((#id = :ref0) AND (#name <> :ref1))',
        ExpressionAttributeNames: {
          '#account': 'account',
          '#id': 'id',
          '#name': 'name',
        },
        ExpressionAttributeValues: {
          ':ref0': '123',
          ':ref1': 'test-name',
        },
      },
    },
    {
      input: {
        and: [{ 'attribute-exists': ['id'] }, {}],
      },
      expect: {
        ConditionExpression: 'attribute_exists(#id)',
        ExpressionAttributeNames: {
          '#id': 'id',
        },
        ExpressionAttributeValues: undefined,
      },
    },
    {
      input: {
        between: {
          name: ['a', 'b'],
        },
        'greater-than': {
          account: 'test-account',
        },
        'greater-than-or-equal-to': {
          id: 'test-id',
        },
        'less-than': {
          name: 'test-name',
        },
        'less-than-or-equal-to': {
          name: 'other-name',
        },
      },
      expect: {
        ConditionExpression:
          '(#name BETWEEN :ref0 AND :ref1) AND #account > :ref2 AND #id >= :ref3 AND #name < :ref4 AND #name <= :ref5',
        ExpressionAttributeNames: {
          '#account': 'account',
          '#id': 'id',
          '#name': 'name',
        },
        ExpressionAttributeValues: {
          ':ref0': 'a',
          ':ref1': 'b',
          ':ref2': 'test-account',
          ':ref3': 'test-id',
          ':ref4': 'test-name',
          ':ref5': 'other-name',
        },
      },
    },
  ];
  CASES.forEach(({ input, expect: expected }, idx) => {
    test(`case ${idx}`, () => {
      const result = serializeExpression(input);
      expect(result).toStrictEqual(expected);
    });
  });
});

describe('serializeUpdate', () => {
  const CASES: {
    input: {
      update: DynamoDBUpdate<TestEntity>;
      condition?: DynamoDBExpression<TestEntity>;
    };
    expect: Partial<ReturnType<typeof serializeUpdate>>;
  }[] = [
    {
      input: {
        update: {
          set: { name: 'new-name', account: 'new-account' },
        },
      },
      expect: {
        ConditionExpression: undefined,
        ExpressionAttributeNames: {
          '#name': 'name',
          '#account': 'account',
        },
        ExpressionAttributeValues: {
          ':ref0': 'new-name',
          ':ref1': 'new-account',
        },
        UpdateExpression: 'SET #name = :ref0, #account = :ref1',
      },
    },
    {
      input: {
        update: {
          set: {
            name: 'new-name',
          },
        },
        condition: {
          or: [
            {
              'attribute-not-exists': ['id'],
              'not-equals': {
                name: 'new-name',
              },
            },
          ],
        },
      },
      expect: {
        ConditionExpression: 'attribute_not_exists(#id) AND #name <> :ref0',
        ExpressionAttributeNames: {
          '#id': 'id',
          '#name': 'name',
        },
        ExpressionAttributeValues: {
          ':ref0': 'new-name',
          ':ref1': 'new-name',
        },
        UpdateExpression: 'SET #name = :ref1',
      },
    },
  ];

  CASES.forEach(({ input, expect: expected }, idx) => {
    test(`case ${idx}`, () => {
      const result = serializeUpdate(input);
      expect(result).toStrictEqual(expected);
    });
  });
});
