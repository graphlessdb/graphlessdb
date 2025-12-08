/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Internal.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBWithTransactionsValidateRequestTests
    {
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicMethods)]
        private static readonly Type AmazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);

        private static void CallValidateRequest(AmazonDynamoDBRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(AmazonDynamoDBRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest method not found");
            method.Invoke(null, new object[] { request });
        }

        private static void CallValidateRequestGetItem(GetItemRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(GetItemRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest(GetItemRequest) method not found");
            method.Invoke(null, new object[] { request });
        }

        private static void CallValidateRequestPutItem(PutItemRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(PutItemRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest(PutItemRequest) method not found");
            method.Invoke(null, new object[] { request });
        }

        private static void CallValidateRequestUpdateItem(UpdateItemRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(UpdateItemRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest(UpdateItemRequest) method not found");
            method.Invoke(null, new object[] { request });
        }

        private static void CallValidateRequestDeleteItem(DeleteItemRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(DeleteItemRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest(DeleteItemRequest) method not found");
            method.Invoke(null, new object[] { request });
        }

        private static void CallValidateRequestTransactGetItems(TransactGetItemsRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(TransactGetItemsRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest(TransactGetItemsRequest) method not found");
            method.Invoke(null, new object[] { request });
        }

        private static void CallValidateRequestTransactWriteItems(TransactWriteItemsRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("ValidateRequest", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(TransactWriteItemsRequest) }, null);

            Assert.IsNotNull(method, "ValidateRequest(TransactWriteItemsRequest) method not found");
            method.Invoke(null, new object[] { request });
        }

        private static bool CallIsKeyNull(TransactWriteItem item)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethod("IsKeyNull", BindingFlags.NonPublic | BindingFlags.Static);

            Assert.IsNotNull(method, "IsKeyNull method not found");
            var result = method.Invoke(null, new object[] { item });
            return (bool)result!;
        }

        private static bool CallHasReservedAttributeTransactGetItem(TransactGetItem item)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
                .FirstOrDefault(m => m.Name == "HasReservedAttribute" && m.GetParameters()[0].ParameterType == typeof(TransactGetItem));

            Assert.IsNotNull(method, "HasReservedAttribute(TransactGetItem) method not found");
            var result = method.Invoke(null, new object[] { item });
            return (bool)result!;
        }

        private static bool CallHasReservedAttributeTransactWriteItem(TransactWriteItem item)
        {
            var method = AmazonDynamoDBWithTransactionsType
                .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
                .FirstOrDefault(m => m.Name == "HasReservedAttribute" && m.GetParameters()[0].ParameterType == typeof(TransactWriteItem));

            Assert.IsNotNull(method, "HasReservedAttribute(TransactWriteItem) method not found");
            var result = method.Invoke(null, new object[] { item });
            return (bool)result!;
        }

        // ValidateRequest(AmazonDynamoDBRequest) Tests

        [TestMethod]
        public void ValidateRequestDispatchesToGetItemRequest()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequest(request);
        }

        [TestMethod]
        public void ValidateRequestDispatchesToPutItemRequest()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequest(request);
        }

        [TestMethod]
        public void ValidateRequestDispatchesToUpdateItemRequest()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequest(request);
        }

        [TestMethod]
        public void ValidateRequestDispatchesToDeleteItemRequest()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequest(request);
        }

        [TestMethod]
        public void ValidateRequestDispatchesToTransactGetItemsRequest()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            CallValidateRequest(request);
        }

        [TestMethod]
        public void ValidateRequestDispatchesToTransactWriteItemsRequest()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            CallValidateRequest(request);
        }

        [TestMethod]
        public void ValidateRequestThrowsNotSupportedExceptionForUnsupportedRequestType()
        {
            var request = new ScanRequest { TableName = "TestTable" };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequest(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
        }

        // ValidateRequest(GetItemRequest) Tests

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenAttributesToGetIsSet()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                AttributesToGet = new List<string> { "name" }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new GetItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenTableNameIsWhitespace()
        {
            var request = new GetItemRequest
            {
                TableName = "   ",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("The request key cannot be empty", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenKeyContainsReservedAttribute()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { ItemAttributeName.TXID.Value, new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenProjectionExpressionContainsReservedAttribute()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ProjectionExpression = $"id, {ItemAttributeName.TXID.Value}"
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#attr", ItemAttributeName.DATE.Value }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestGetItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateGetItemRequestSucceedsWithValidRequest()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequestGetItem(request);
        }

        // ValidateRequest(PutItemRequest) Tests

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenConditionalOperatorIsSet()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ConditionalOperator = ConditionalOperator.AND
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestPutItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenExpectedIsSet()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                Expected = new Dictionary<string, ExpectedAttributeValue>
                {
                    { "id", new ExpectedAttributeValue { Exists = false } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestPutItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new PutItemRequest
            {
                TableName = null,
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestPutItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenItemContainsReservedAttribute()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "test" } },
                    { ItemAttributeName.TRANSIENT.Value, new AttributeValue { S = "reserved" } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestPutItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenConditionExpressionContainsReservedAttribute()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ConditionExpression = $"attribute_exists({ItemAttributeName.APPLIED.Value})"
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestPutItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#attr", ItemAttributeName.TXID.Value }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestPutItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidatePutItemRequestSucceedsWithValidRequest()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequestPutItem(request);
        }

        // ValidateRequest(UpdateItemRequest) Tests

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenConditionalOperatorIsSet()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ConditionalOperator = ConditionalOperator.OR
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenExpectedIsSet()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                Expected = new Dictionary<string, ExpectedAttributeValue>
                {
                    { "id", new ExpectedAttributeValue { Exists = true } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenAttributeUpdatesIsSet()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
                {
                    { "name", new AttributeValueUpdate { Action = AttributeAction.PUT, Value = new AttributeValue { S = "test" } } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new UpdateItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("The request key cannot be empty", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenKeyContainsReservedAttribute()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { ItemAttributeName.DATE.Value, new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenConditionExpressionContainsReservedAttribute()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ConditionExpression = $"{ItemAttributeName.TXID.Value} = :val"
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenUpdateExpressionContainsReservedAttribute()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                UpdateExpression = $"SET {ItemAttributeName.TRANSIENT.Value} = :val"
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#attr", ItemAttributeName.APPLIED.Value }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestUpdateItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateUpdateItemRequestSucceedsWithValidRequest()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequestUpdateItem(request);
        }

        // ValidateRequest(DeleteItemRequest) Tests

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenConditionalOperatorIsSet()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ConditionalOperator = ConditionalOperator.AND
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenExpectedIsSet()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                Expected = new Dictionary<string, ExpectedAttributeValue>
                {
                    { "id", new ExpectedAttributeValue { Exists = true } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
            Assert.AreEqual("Legacy attributes on requests are not supported", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new DeleteItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("The request key cannot be empty", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenKeyContainsReservedAttribute()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { ItemAttributeName.APPLIED.Value, new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenConditionExpressionContainsReservedAttribute()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ConditionExpression = $"attribute_not_exists({ItemAttributeName.TXID.Value})"
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#attr", ItemAttributeName.DATE.Value }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestDeleteItem(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateDeleteItemRequestSucceedsWithValidRequest()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
            };

            CallValidateRequestDeleteItem(request);
        }

        // ValidateRequest(TransactGetItemsRequest) Tests

        [TestMethod]
        public void ValidateTransactGetItemsRequestThrowsWhenTableNameIsNull()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = null,
                            Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactGetItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactGetItemsRequestThrowsWhenKeyIsEmpty()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactGetItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("The request key cannot be empty", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactGetItemsRequestThrowsWhenContainsReservedAttribute()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { ItemAttributeName.TXID.Value, new AttributeValue { S = "test" } }
                            }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactGetItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactGetItemsRequestSucceedsWithValidRequest()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            CallValidateRequestTransactGetItems(request);
        }

        // ValidateRequest(TransactWriteItemsRequest) Tests

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenPutTableNameIsNull()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = null,
                            Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactWriteItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenDeleteTableNameIsNull()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = null,
                            Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactWriteItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenUpdateTableNameIsNull()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = null,
                            Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactWriteItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenConditionCheckTableNameIsNull()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = null,
                            Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactWriteItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("TableName must not be null", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenKeyIsNull()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactWriteItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("The request key cannot be empty", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenContainsReservedAttribute()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "id", new AttributeValue { S = "test" } },
                                { ItemAttributeName.TXID.Value, new AttributeValue { S = "reserved" } }
                            }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallValidateRequestTransactWriteItems(request));
            Assert.IsInstanceOfType<InvalidOperationException>(exception.InnerException);
            Assert.AreEqual("Request must not contain a reserved attribute", exception.InnerException.Message);
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestSucceedsWithValidPutRequest()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            CallValidateRequestTransactWriteItems(request);
        }

        // IsKeyNull Tests

        [TestMethod]
        public void IsKeyNullReturnsTrueForConditionCheckWithEmptyKey()
        {
            var item = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>()
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsKeyNullReturnsFalseForConditionCheckWithKey()
        {
            var item = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsKeyNullReturnsTrueForDeleteWithEmptyKey()
        {
            var item = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>()
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsKeyNullReturnsFalseForDeleteWithKey()
        {
            var item = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsKeyNullReturnsFalseForPut()
        {
            var item = new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = "TestTable",
                    Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsKeyNullReturnsTrueForUpdateWithEmptyKey()
        {
            var item = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>()
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsKeyNullReturnsFalseForUpdateWithKey()
        {
            var item = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallIsKeyNull(item);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsKeyNullThrowsForInvalidTransactWriteItem()
        {
            var item = new TransactWriteItem();

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallIsKeyNull(item));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
        }

        // HasReservedAttribute(TransactGetItem) Tests

        [TestMethod]
        public void HasReservedAttributeTransactGetItemReturnsTrueWhenKeyContainsReservedAttribute()
        {
            var item = new TransactGetItem
            {
                Get = new Get
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { ItemAttributeName.TXID.Value, new AttributeValue { S = "test" } }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactGetItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactGetItemReturnsTrueWhenProjectionExpressionContainsReservedAttribute()
        {
            var item = new TransactGetItem
            {
                Get = new Get
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ProjectionExpression = $"id, {ItemAttributeName.DATE.Value}"
                }
            };

            var result = CallHasReservedAttributeTransactGetItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactGetItemReturnsTrueWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var item = new TransactGetItem
            {
                Get = new Get
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#attr", ItemAttributeName.TRANSIENT.Value }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactGetItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactGetItemReturnsFalseForValidItem()
        {
            var item = new TransactGetItem
            {
                Get = new Get
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallHasReservedAttributeTransactGetItem(item);
            Assert.IsFalse(result);
        }

        // HasReservedAttribute(TransactWriteItem) - ConditionCheck Tests

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemConditionCheckReturnsTrueWhenKeyContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { ItemAttributeName.APPLIED.Value, new AttributeValue { S = "test" } }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemConditionCheckReturnsTrueWhenConditionExpressionContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ConditionExpression = $"attribute_exists({ItemAttributeName.TXID.Value})"
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemConditionCheckReturnsTrueWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#attr", ItemAttributeName.DATE.Value }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemConditionCheckReturnsFalseForValidItem()
        {
            var item = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsFalse(result);
        }

        // HasReservedAttribute(TransactWriteItem) - Delete Tests

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemDeleteReturnsTrueWhenKeyContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { ItemAttributeName.TRANSIENT.Value, new AttributeValue { S = "test" } }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemDeleteReturnsTrueWhenConditionExpressionContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ConditionExpression = $"{ItemAttributeName.APPLIED.Value} = :val"
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemDeleteReturnsTrueWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#attr", ItemAttributeName.TXID.Value }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemDeleteReturnsFalseForValidItem()
        {
            var item = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsFalse(result);
        }

        // HasReservedAttribute(TransactWriteItem) - Put Tests

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemPutReturnsTrueWhenItemContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = "TestTable",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue { S = "test" } },
                        { ItemAttributeName.DATE.Value, new AttributeValue { S = "reserved" } }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemPutReturnsTrueWhenConditionExpressionContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = "TestTable",
                    Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ConditionExpression = $"attribute_not_exists({ItemAttributeName.TRANSIENT.Value})"
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemPutReturnsTrueWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = "TestTable",
                    Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#attr", ItemAttributeName.APPLIED.Value }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemPutReturnsFalseForValidItem()
        {
            var item = new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = "TestTable",
                    Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsFalse(result);
        }

        // HasReservedAttribute(TransactWriteItem) - Update Tests

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemUpdateReturnsTrueWhenKeyContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { ItemAttributeName.TXID.Value, new AttributeValue { S = "test" } }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemUpdateReturnsTrueWhenConditionExpressionContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ConditionExpression = $"{ItemAttributeName.DATE.Value} > :val"
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemUpdateReturnsTrueWhenExpressionAttributeNamesContainsReservedAttribute()
        {
            var item = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } },
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#attr", ItemAttributeName.TRANSIENT.Value }
                    }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemUpdateReturnsFalseForValidItem()
        {
            var item = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "test" } } }
                }
            };

            var result = CallHasReservedAttributeTransactWriteItem(item);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasReservedAttributeTransactWriteItemThrowsForInvalidItem()
        {
            var item = new TransactWriteItem();

            var exception = Assert.ThrowsException<TargetInvocationException>(() => CallHasReservedAttributeTransactWriteItem(item));
            Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
        }
    }
}
