import {
  PutCommandInput,
  QueryCommandInput,
  UpdateCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { SetRequired } from 'type-fest';

import { KeySchema } from './';

type BaseDynamoDBCondition<Entity> = {
  'attribute-exists'?: (keyof Entity)[];
  'attribute-not-exists'?: (keyof Entity)[];
  equals?: {
    [Key in keyof Entity]?: Entity[Key];
  };
  'not-equals'?: {
    [Key in keyof Entity]?: Entity[Key];
  };
  between?: {
    [Key in keyof Entity]?: [Entity[Key], Entity[Key]];
  };
  'begins-with'?: {
    [Key in keyof Entity]?: Entity[Key];
  };
  'greater-than'?: {
    [Key in keyof Entity]?: Entity[Key];
  };
  'greater-than-or-equal-to'?: {
    [Key in keyof Entity]?: Entity[Key];
  };
  'less-than'?: {
    [Key in keyof Entity]?: Entity[Key];
  };
  'less-than-or-equal-to'?: {
    [Key in keyof Entity]?: Entity[Key];
  };
};

/**
 * This object describes serializers for each of the operators we support.
 */
const Serializers: {
  [Key in keyof BaseDynamoDBCondition<any>]-?: (
    ctx: Pick<RefContext, 'getSubjectRef' | 'getObjectRef'>,
    condition: NonNullable<BaseDynamoDBCondition<any>[Key]>,
  ) => string[];
} = {
  'attribute-exists': (ctx, keys) =>
    keys.map((key) => `attribute_exists(${ctx.getSubjectRef(key as string)})`),
  'attribute-not-exists': (ctx, keys) =>
    keys.map(
      (key) => `attribute_not_exists(${ctx.getSubjectRef(key as string)})`,
    ),
  equals: (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `${ctx.getSubjectRef(subject)} = ${ctx.getObjectRef(object)}`,
    ),
  'not-equals': (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `${ctx.getSubjectRef(subject)} <> ${ctx.getObjectRef(object)}`,
    ),
  between: (ctx, entries) =>
    Object.entries(entries).map(
      // @ts-expect-error
      ([subject, [from, to]]) =>
        `(${ctx.getSubjectRef(subject)} BETWEEN ${ctx.getObjectRef(
          from,
        )} AND ${ctx.getObjectRef(to)})`,
    ),
  'begins-with': (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `begins_with(${ctx.getSubjectRef(subject)}, ${ctx.getObjectRef(
          object,
        )})`,
    ),
  'greater-than': (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `${ctx.getSubjectRef(subject)} > ${ctx.getObjectRef(object)}`,
    ),
  'greater-than-or-equal-to': (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `${ctx.getSubjectRef(subject)} >= ${ctx.getObjectRef(object)}`,
    ),
  'less-than': (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `${ctx.getSubjectRef(subject)} < ${ctx.getObjectRef(object)}`,
    ),
  'less-than-or-equal-to': (ctx, entries) =>
    Object.entries(entries).map(
      ([subject, object]) =>
        `${ctx.getSubjectRef(subject)} <= ${ctx.getObjectRef(object)}`,
    ),
};

/**
 * A DynamoDB condition is a way to express a set of conditions that must be
 * met in order for a DynamoDB operation to succeed.
 *
 * When using this custom conditions syntax, follow these guidelines:
 *
 * - In a single condition _object_, all conditions are AND-ed together.
 *
 * - Conditions can also be AND-ed together using the `and` operator.
 *
 * - Conditions can be `OR-ed` together using the `or` operator.
 *
 * @example
 *
 * // Checks that:
 * // - the `user` attribute exists
 * // AND
 * // - the `firstName` attribute is equal to "Jane".
 * const condition = {
 *   'attribute-exists': ['user'],
 *   equals: {
 *     firstName: 'Jane'
 *   }
 * }
 *
 * // This condition is _identical_ to the one above.
 * const condition = {
 *   and: [
 *     { 'attribute-exists': ['user'] },
 *     { equals: { firstName: 'Jane' }
 *   ]
 * }
 *
 * // This condition checks that:
 * // - the user attribute exists
 * // OR
 * // - the `firstName` attribute is equal to "Jane", AND the `lastName`
 * //   attribute is equal to "Doe".
 * const condition = {
 *   or: [
 *     { 'attribute-exists': ['user'] },
 *     { equals: { firstName: 'Jane', lastName: 'Doe' }
 *   ]
 * }
 */
export type DynamoDBCondition<Entity> =
  | BaseDynamoDBCondition<Entity>
  | { and: DynamoDBCondition<Entity>[] }
  | { or: DynamoDBCondition<Entity>[] };

const joinConditionsUsing = (
  context: Pick<RefContext, 'getSubjectRef' | 'getObjectRef'>,
  conditions: DynamoDBCondition<any>[],
  joiner: 'OR' | 'AND',
) => {
  const serialized = conditions
    .map((cond) => _serializeCondition(cond, context))
    .filter(Boolean);

  if (serialized.length === 1) {
    return serialized[0];
  }

  // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
  return serialized.map((cond) => `(${cond})`).join(` ${joiner} `);
};

const _serializeCondition = <Entity>(
  condition: DynamoDBCondition<Entity>,
  context: Pick<RefContext, 'getSubjectRef' | 'getObjectRef'>,
): string | undefined => {
  if ('or' in condition) {
    return joinConditionsUsing(context, condition.or, 'OR');
  }

  if ('and' in condition) {
    return joinConditionsUsing(context, condition.and, 'AND');
  }

  // We'll build the condition list + attribute values through iterations.
  const conditions: string[] = [];

  for (const key in condition) {
    const value = condition[key as keyof typeof condition];
    if (!value) {
      continue;
    }
    const serializer = Serializers[key as keyof typeof Serializers];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    conditions.push(...serializer(context, value as any));
  }

  if (!conditions.length) {
    return undefined;
  }

  return conditions.join(' AND ');
};

/**
 * One key problem when supporting arbitrary DynamoDB expressions is that we
 * need some way to manage references to the different "object" values being
 * provided in the expressions.
 *
 * This function creates a single "context" that can be used to manage these
 * refs, ensuring that each new ref is unique + accounted for.
 *
 * A single DynamoDB operation should only ever use _one_ context. Using
 * multiple contexts in a single operation will result in duplicate refs,
 * causing undesired behavior.
 */
type RefContext = {
  getSubjectRef: (subject: string) => string;
  getObjectRef: (object: any) => string;
  ExpressionAttributeNames: Record<string, string>;
  ExpressionAttributeValues: Record<string, any>;
};

const createRefContext = (): RefContext => {
  const ExpressionAttributeNames: Record<string, string> = {};
  const getSubjectRef = (subject: string) => {
    const ref = `#${subject}`;
    ExpressionAttributeNames[ref] = subject;
    return ref;
  };

  const ExpressionAttributeValues: Record<string, any> = {};
  let objectRefCounter = 0;
  const getObjectRef = (object: any) => {
    const ref = `:ref${objectRefCounter++}`;
    ExpressionAttributeValues[ref] = object;
    return ref;
  };

  return {
    getSubjectRef,
    getObjectRef,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
  };
};

/**
 * Returns DynamoDB client parameters describing the specified condition.
 */
export const serializeCondition = <Entity>(
  condition: DynamoDBCondition<Entity> = {},
): Pick<
  PutCommandInput,
  | 'ConditionExpression'
  | 'ExpressionAttributeNames'
  | 'ExpressionAttributeValues'
> => {
  const {
    getSubjectRef,
    getObjectRef,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
  } = createRefContext();

  const ConditionExpression = _serializeCondition(condition, {
    getSubjectRef,
    getObjectRef,
  });

  if (!ConditionExpression) {
    return {};
  }

  return {
    ConditionExpression,
    ExpressionAttributeNames,
    // Dynamo will throw if this value is {}. So, check for that before returning.
    ExpressionAttributeValues: Object.keys(ExpressionAttributeValues).length
      ? ExpressionAttributeValues
      : undefined,
  };
};

/**
 * A description of a DynamoDB update operation.
 */
export type DynamoDBUpdate<Entity> = {
  /**
   * A patch to apply to the record.
   */
  set: Partial<Entity>;
  // TODO: support remove
};

const _serializeUpdate = <Entity>(
  update: DynamoDBUpdate<Entity>,
  context: Pick<RefContext, 'getSubjectRef' | 'getObjectRef'>,
): string => {
  const expressions: string[] = [];

  const operations = Object.entries(update.set).map(
    ([key, value]) =>
      `${context.getSubjectRef(key)} = ${context.getObjectRef(value)}`,
  );
  expressions.push(`SET ` + operations.join(', '));

  return expressions.join(' ');
};

export type SerializeUpdateParams<Entity> = {
  /** The update expression to serialize. */
  update: DynamoDBUpdate<Entity>;
  /** A condition expression to include. */
  condition?: DynamoDBCondition<Entity>;
};

/**
 * Returns DynamoDB client parameters describing the specified update.
 */
export const serializeUpdate = <Entity>({
  update,
  condition,
}: SerializeUpdateParams<Entity>): SetRequired<
  Pick<
    UpdateCommandInput,
    | 'UpdateExpression'
    | 'ConditionExpression'
    | 'ExpressionAttributeNames'
    | 'ExpressionAttributeValues'
  >,
  'UpdateExpression'
> => {
  const {
    getSubjectRef,
    getObjectRef,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
  } = createRefContext();

  const ConditionExpression = condition
    ? _serializeCondition(condition, {
        getSubjectRef,
        getObjectRef,
      })
    : undefined;

  const UpdateExpression = _serializeUpdate(update, {
    getSubjectRef,
    getObjectRef,
  });

  return {
    UpdateExpression,
    ConditionExpression,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
  };
};

export type RangeKeyCondition<Entity, Range extends keyof Entity> =
  | {
      between?: [Entity[Range], Entity[Range]];
      'begins-with'?: Entity[Range];
      'greater-than'?: Entity[Range];
      'greater-than-or-equal-to'?: Entity[Range];
      'less-than'?: Entity[Range];
      'less-than-or-equal-to'?: Entity[Range];
    }
  | { and: RangeKeyCondition<Entity, Range>[] }
  | { or: RangeKeyCondition<Entity, Range>[] };

export type KeyCondition<Entity, Keys extends KeySchema<Entity>> = {
  [Key in Keys['hash']]: Entity[Keys['hash']];
} & (undefined extends Keys['range']
  ? {}
  : {
      [RangeKey in NonNullable<Keys['range']>]?: RangeKeyCondition<
        Entity,
        NonNullable<Keys['range']>
      >;
    });

const serializeRangeKeyCondition = <Entity>(
  key: string,
  rangeKeyCondition: RangeKeyCondition<Entity, any>,
): DynamoDBCondition<Entity> => {
  if ('or' in rangeKeyCondition) {
    return {
      or: rangeKeyCondition.or.map((cond) =>
        serializeRangeKeyCondition(key, cond),
      ),
    };
  }

  if ('and' in rangeKeyCondition) {
    return {
      and: rangeKeyCondition.and.map((cond) =>
        serializeRangeKeyCondition(key, cond),
      ),
    };
  }

  const condition: DynamoDBCondition<Entity> = {};

  for (const [operator, value] of Object.entries(rangeKeyCondition)) {
    // @ts-expect-error
    condition[operator] = { [key]: value };
  }

  return condition;
};

export const serializeKeyCondition = <Entity, Keys extends KeySchema<Entity>>(
  schema: Keys,
  keyCondition: KeyCondition<Entity, Keys>,
): Pick<
  QueryCommandInput,
  | 'KeyConditionExpression'
  | 'ExpressionAttributeNames'
  | 'ExpressionAttributeValues'
> => {
  const hashKeyCondition: DynamoDBCondition<any> = {
    equals: { [schema.hash]: keyCondition[schema.hash] },
  };

  const rangeKeyCondition = schema.range
    ? keyCondition[schema.range]
    : undefined;

  const condition = rangeKeyCondition
    ? {
        and: [
          hashKeyCondition,
          serializeRangeKeyCondition(schema.range as string, rangeKeyCondition),
        ],
      }
    : hashKeyCondition;

  const serialized = serializeCondition(condition);

  return {
    KeyConditionExpression: serialized.ConditionExpression,
    ExpressionAttributeNames: serialized.ExpressionAttributeNames,
    ExpressionAttributeValues: serialized.ExpressionAttributeValues,
  };
};
