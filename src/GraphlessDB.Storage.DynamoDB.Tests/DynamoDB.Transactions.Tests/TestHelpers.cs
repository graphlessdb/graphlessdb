/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.Collections.Generic;
using GraphlessDB.DynamoDB.Transactions.Storage;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    public static class TestHelpers
    {
        public static bool AreEqual(RequestRecord a, RequestRecord b)
        {
            if (a.Id != b.Id)
            {
                return false;
            }

            if (a.GetItemRequest != null)
            {
                return b.GetItemRequest != null && AreEqual(a.GetItemRequest, b.GetItemRequest);
            }

            if (a.PutItemRequest != null)
            {
                return b.PutItemRequest != null && AreEqual(a.PutItemRequest, b.PutItemRequest);
            }

            if (a.UpdateItemRequest != null)
            {
                return b.UpdateItemRequest != null && AreEqual(a.UpdateItemRequest, b.UpdateItemRequest);
            }

            if (a.DeleteItemRequest != null)
            {
                return b.DeleteItemRequest != null && AreEqual(a.DeleteItemRequest, b.DeleteItemRequest);
            }

            throw new InvalidOperationException();
        }

        public static bool AreEqual(GetItemRequest a, GetItemRequest b)
        {
            return
                a.AttributesToGet.SequenceEqual(b.AttributesToGet) &&
                a.ConsistentRead == b.ConsistentRead &&
                a.ExpressionAttributeNames.SequenceEqual(b.ExpressionAttributeNames) &&
                AreEqual(a.Key, b.Key) &&
                a.ProjectionExpression == b.ProjectionExpression &&
                a.ReturnConsumedCapacity == b.ReturnConsumedCapacity &&
                a.TableName == b.TableName;
        }

        public static bool AreEqual(PutItemRequest a, PutItemRequest b)
        {
            return
                a.ConditionalOperator == b.ConditionalOperator &&
                a.ConditionExpression == b.ConditionExpression &&
                AreEqual(a.Expected, b.Expected) &&
                a.ExpressionAttributeNames.SequenceEqual(b.ExpressionAttributeNames) &&
                AreEqual(a.ExpressionAttributeValues, b.ExpressionAttributeValues) &&
                AreEqual(a.Item, b.Item) &&
                a.ReturnConsumedCapacity == b.ReturnConsumedCapacity &&
                a.ReturnItemCollectionMetrics == b.TableName &&
                a.ReturnValues == b.TableName &&
                a.TableName == b.TableName;
        }

        public static bool AreEqual(UpdateItemRequest a, UpdateItemRequest b)
        {
            return
                AreEqual(a.Expected, b.Expected) &&
                a.ExpressionAttributeNames.SequenceEqual(b.ExpressionAttributeNames) &&
                AreEqual(a.ExpressionAttributeValues, b.ExpressionAttributeValues) &&
                AreEqual(a.Key, b.Key) &&
                a.ReturnConsumedCapacity == b.ReturnConsumedCapacity &&
                a.ReturnItemCollectionMetrics == b.ReturnItemCollectionMetrics &&
                a.ReturnValues == b.ReturnValues &&
                a.TableName == b.TableName &&
                a.UpdateExpression == b.UpdateExpression;
        }

        public static bool AreEqual(DeleteItemRequest a, DeleteItemRequest b)
        {
            return
                a.ConditionalOperator == b.ConditionalOperator &&
                a.ConditionExpression == b.ConditionExpression &&
                a.ExpressionAttributeNames.SequenceEqual(b.ExpressionAttributeNames) &&
                AreEqual(a.ExpressionAttributeValues, b.ExpressionAttributeValues) &&
                AreEqual(a.Key, b.Key) &&
                a.ReturnConsumedCapacity == b.ReturnConsumedCapacity &&
                a.ReturnItemCollectionMetrics == b.ReturnItemCollectionMetrics &&
                a.ReturnValues == b.ReturnValues &&
                a.TableName == b.TableName;
        }

        public static bool AreEqual(IEnumerable<KeyValuePair<string, ExpectedAttributeValue>> a, IEnumerable<KeyValuePair<string, ExpectedAttributeValue>> b)
        {
            return a.OrderBy(k => k.Key).SequenceEqual(b.OrderBy(k => k.Key), new FuncEqualityComparer<KeyValuePair<string, ExpectedAttributeValue>>(
                (x, y) => x.Key == y.Key && AreEqual(x.Value, y.Value),
                x => HashCode.Combine(x.Key, GetHashCode(x.Value))));
        }

        public static bool AreEqual(IEnumerable<AttributeValue> a, IEnumerable<AttributeValue> b)
        {
            return a.SequenceEqual(b, new FuncEqualityComparer<AttributeValue>(
                (x, y) => x != null && y != null && AreEqual(x, y), GetHashCode));
        }

        public static bool AreEqual(IEnumerable<KeyValuePair<string, AttributeValue>> a, IEnumerable<KeyValuePair<string, AttributeValue>> b)
        {
            return a.OrderBy(k => k.Key).SequenceEqual(b.OrderBy(k => k.Key), new FuncEqualityComparer<KeyValuePair<string, AttributeValue>>(
                (x, y) => x.Key == y.Key && AreEqual(x.Value, y.Value),
                x => HashCode.Combine(x.Key, GetHashCode(x.Value))));
        }

        public static bool AreEqual(ExpectedAttributeValue a, ExpectedAttributeValue b)
        {
            if (!a.AttributeValueList.Select(ImmutableAttributeValue.Create).SequenceEqual(b.AttributeValueList.Select(ImmutableAttributeValue.Create)))
            {
                return false;
            }

            if (a.ComparisonOperator != b.ComparisonOperator)
            {
                return false;
            }

            if (a.Exists != b.Exists)
            {
                return false;
            }

            return ImmutableAttributeValue.Create(a.Value) == ImmutableAttributeValue.Create(b.Value);
        }

        public static bool AreEqual(AttributeValue a, AttributeValue b)
        {
            var immutableA = ImmutableAttributeValue.Create(a);
            var immutableB = ImmutableAttributeValue.Create(b);
            var areEqual = immutableA == immutableB;
            return areEqual;
        }

        public static int GetHashCode(ExpectedAttributeValue value)
        {
            return HashCode.Combine(value.ComparisonOperator, value.Exists, GetHashCode(value.Value), GetHashCode(value.AttributeValueList));
        }

        public static int GetHashCode(IEnumerable<AttributeValue> values)
        {
            unchecked
            {
                var hash = 19;
                foreach (var value in values)
                {
                    hash = hash * 31 + GetHashCode(value);
                }
                return hash;
            }
        }


        public static int GetHashCode(AttributeValue value)
        {
            return HashCode.Combine(value.BOOL, value.N, value.NULL, value.S);
        }
    }
}
