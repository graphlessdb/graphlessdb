/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Internal.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBWithTransactionsNoTransactionConditionTests
    {
        [TestMethod]
        public async Task WithNoExistingTransactionConditionDeleteWithoutExistingConditionExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            var deleteItem = capturedRequest.TransactItems[0].Delete;
            Assert.IsNotNull(deleteItem);
            Assert.AreEqual("TestTable", deleteItem.TableName);
            Assert.IsTrue(deleteItem.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(deleteItem.ConditionExpression.Contains("attribute_not_exists(#_TxA)"));
            Assert.IsTrue(deleteItem.ConditionExpression.Contains("attribute_not_exists(#_TxT)"));
            Assert.IsTrue(deleteItem.ExpressionAttributeNames.ContainsKey("#_TxId"));
            Assert.IsTrue(deleteItem.ExpressionAttributeNames.ContainsKey("#_TxA"));
            Assert.IsTrue(deleteItem.ExpressionAttributeNames.ContainsKey("#_TxT"));
            Assert.AreEqual("_TxId", deleteItem.ExpressionAttributeNames["#_TxId"]);
            Assert.AreEqual("_TxA", deleteItem.ExpressionAttributeNames["#_TxA"]);
            Assert.AreEqual("_TxT", deleteItem.ExpressionAttributeNames["#_TxT"]);
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionDeleteWithExistingConditionExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            ConditionExpression = "attribute_exists(#name)",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#name", "Name" } }
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var deleteItem = capturedRequest.TransactItems[0].Delete;
            Assert.IsNotNull(deleteItem);
            Assert.IsTrue(deleteItem.ConditionExpression.Contains("attribute_exists(#name)"));
            Assert.IsTrue(deleteItem.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(deleteItem.ConditionExpression.Contains(" AND "));
            Assert.IsTrue(deleteItem.ExpressionAttributeNames.ContainsKey("#name"));
            Assert.IsTrue(deleteItem.ExpressionAttributeNames.ContainsKey("#_TxId"));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionDeletePreservesReturnValuesOnConditionCheckFailure()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            ExpressionAttributeNames = new Dictionary<string, string>(),
                            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var deleteItem = capturedRequest.TransactItems[0].Delete;
            Assert.IsNotNull(deleteItem);
            Assert.AreEqual(ReturnValuesOnConditionCheckFailure.ALL_OLD, deleteItem.ReturnValuesOnConditionCheckFailure);
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionPutWithoutExistingConditionExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", new AttributeValue("test-id") },
                                { "Name", new AttributeValue("Test") }
                            },
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var putItem = capturedRequest.TransactItems[0].Put;
            Assert.IsNotNull(putItem);
            Assert.AreEqual("TestTable", putItem.TableName);
            Assert.AreEqual(2, putItem.Item.Count);
            Assert.IsTrue(putItem.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(putItem.ConditionExpression.Contains("attribute_not_exists(#_TxA)"));
            Assert.IsTrue(putItem.ConditionExpression.Contains("attribute_not_exists(#_TxT)"));
            Assert.IsTrue(putItem.ExpressionAttributeNames.ContainsKey("#_TxId"));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionPutWithExistingConditionExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            ConditionExpression = "attribute_not_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var putItem = capturedRequest.TransactItems[0].Put;
            Assert.IsNotNull(putItem);
            Assert.IsTrue(putItem.ConditionExpression.Contains("attribute_not_exists(Id)"));
            Assert.IsTrue(putItem.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(putItem.ConditionExpression.Contains(" AND "));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionPutPreservesItemAndReturnValues()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", new AttributeValue("test-id") },
                { "Name", new AttributeValue("Test") },
                { "Value", new AttributeValue { N = "42" } }
            };

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = item,
                            ExpressionAttributeNames = new Dictionary<string, string>(),
                            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var putItem = capturedRequest.TransactItems[0].Put;
            Assert.IsNotNull(putItem);
            Assert.AreEqual(3, putItem.Item.Count);
            Assert.AreEqual("test-id", putItem.Item["Id"].S);
            Assert.AreEqual("Test", putItem.Item["Name"].S);
            Assert.AreEqual("42", putItem.Item["Value"].N);
            Assert.AreEqual(ReturnValuesOnConditionCheckFailure.ALL_OLD, putItem.ReturnValuesOnConditionCheckFailure);
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionUpdateWithoutExistingConditionExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            UpdateExpression = "SET #name = :name",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#name", "Name" } },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":name", new AttributeValue("NewName") } }
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var updateItem = capturedRequest.TransactItems[0].Update;
            Assert.IsNotNull(updateItem);
            Assert.AreEqual("TestTable", updateItem.TableName);
            Assert.AreEqual("SET #name = :name", updateItem.UpdateExpression);
            Assert.IsTrue(updateItem.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(updateItem.ConditionExpression.Contains("attribute_not_exists(#_TxA)"));
            Assert.IsTrue(updateItem.ConditionExpression.Contains("attribute_not_exists(#_TxT)"));
            Assert.IsTrue(updateItem.ExpressionAttributeNames.ContainsKey("#name"));
            Assert.IsTrue(updateItem.ExpressionAttributeNames.ContainsKey("#_TxId"));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionUpdateWithExistingConditionExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            UpdateExpression = "SET #name = :name",
                            ConditionExpression = "attribute_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#name", "Name" } },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":name", new AttributeValue("NewName") } }
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var updateItem = capturedRequest.TransactItems[0].Update;
            Assert.IsNotNull(updateItem);
            Assert.IsTrue(updateItem.ConditionExpression.Contains("attribute_exists(Id)"));
            Assert.IsTrue(updateItem.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(updateItem.ConditionExpression.Contains(" AND "));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionUpdatePreservesAllFields()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            UpdateExpression = "SET #name = :name, #value = :value",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#name", "Name" },
                                { "#value", "Value" }
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":name", new AttributeValue("NewName") },
                                { ":value", new AttributeValue { N = "100" } }
                            },
                            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var updateItem = capturedRequest.TransactItems[0].Update;
            Assert.IsNotNull(updateItem);
            Assert.AreEqual("SET #name = :name, #value = :value", updateItem.UpdateExpression);
            Assert.AreEqual(5, updateItem.ExpressionAttributeNames.Count);
            Assert.AreEqual(2, updateItem.ExpressionAttributeValues.Count);
            Assert.AreEqual(ReturnValuesOnConditionCheckFailure.ALL_OLD, updateItem.ReturnValuesOnConditionCheckFailure);
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionConditionCheckWithSupportedExpression()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            ConditionExpression = "attribute_not_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            var conditionCheck = capturedRequest.TransactItems[0].ConditionCheck;
            Assert.IsNotNull(conditionCheck);
            Assert.AreEqual("TestTable", conditionCheck.TableName);
            Assert.IsTrue(conditionCheck.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(conditionCheck.ConditionExpression.Contains("attribute_not_exists(#_TxA)"));
            Assert.IsTrue(conditionCheck.ConditionExpression.Contains("attribute_not_exists(#_TxT)"));
            Assert.IsTrue(conditionCheck.ExpressionAttributeNames.ContainsKey("#_TxId"));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionConditionCheckThrowsForUnsupportedExpression()
        {
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                            ConditionExpression = "attribute_exists(Name)",
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    }
                ]
            };

            await Assert.ThrowsExceptionAsync<NotSupportedException>(() =>
                service.TransactWriteItemsAsync(request, CancellationToken.None));
        }

        [TestMethod]
        public async Task WithNoExistingTransactionConditionMixedItemTypes()
        {
            TransactWriteItemsRequest? capturedRequest = null;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var service = CreateService(mockDynamoDB);

            var request = new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "Table1",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("id1") } },
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    },
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "Table2",
                            Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("id2") } },
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    },
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "Table3",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("id3") } },
                            UpdateExpression = "SET #n = :v",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#n", "Name" } },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":v", new AttributeValue("value") } }
                        }
                    }
                ]
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(3, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Delete);
            Assert.IsNotNull(capturedRequest.TransactItems[1].Put);
            Assert.IsNotNull(capturedRequest.TransactItems[2].Update);
            Assert.IsTrue(capturedRequest.TransactItems[0].Delete.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(capturedRequest.TransactItems[1].Put.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
            Assert.IsTrue(capturedRequest.TransactItems[2].Update.ConditionExpression.Contains("attribute_not_exists(#_TxId)"));
        }

        [TestMethod]
        public void CombineWithMultipleNonNullExpressions()
        {
            var result = Combine("expr1", "expr2", "expr3");
            Assert.AreEqual("expr1 AND expr2 AND expr3", result);
        }

        [TestMethod]
        public void CombineWithNullExpressions()
        {
            var result = Combine("expr1", null, "expr3");
            Assert.AreEqual("expr1 AND expr3", result);
        }

        [TestMethod]
        public void CombineWithWhitespaceExpressions()
        {
            var result = Combine("expr1", "  ", "expr3");
            Assert.AreEqual("expr1 AND expr3", result);
        }

        [TestMethod]
        public void CombineWithAllNullInputs()
        {
            var result = Combine(null, null, null);
            Assert.AreEqual("", result);
        }

        [TestMethod]
        public void CombineTrimsExpressions()
        {
            var result = Combine("  expr1  ", "  expr2  ");
            Assert.AreEqual("expr1 AND expr2", result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionWithMatchingCondition()
        {
            var conditionCheck = new ConditionCheck
            {
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                ConditionExpression = "attribute_not_exists(Id)"
            };

            var result = IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionWithNonMatchingCondition()
        {
            var conditionCheck = new ConditionCheck
            {
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue("test-id") } },
                ConditionExpression = "attribute_exists(Id)"
            };

            var result = IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionWithMultipleKeys()
        {
            var conditionCheck = new ConditionCheck
            {
                Key = new Dictionary<string, AttributeValue>
                {
                    { "PartitionKey", new AttributeValue("pk") },
                    { "SortKey", new AttributeValue("sk") }
                },
                ConditionExpression = "attribute_not_exists(PartitionKey)"
            };

            var result = IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsTrue(result);
        }

        private static AmazonDynamoDBWithTransactions CreateService(MockAmazonDynamoDB mockDynamoDB)
        {
            var options = new AmazonDynamoDBOptions
            {
                TransactionStaleDuration = TimeSpan.FromMinutes(5),
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100,
                TransactGetItemCountMaxValue = 100
            };

            var optionsSnapshot = new MockOptionsSnapshot<AmazonDynamoDBOptions>(options);

            return new AmazonDynamoDBWithTransactions(
                optionsSnapshot,
                mockDynamoDB,
                new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>(),
                new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>(),
                new MockTransactionStore(),
                new MockVersionedItemStore(),
                new MockItemImageStore(),
                new MockRequestService(),
                new MockTransactionServiceEvents(),
                new MockFullyAppliedRequestService());
        }

        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            public IDynamoDBv2PaginatorFactory Paginators { get; set; } = null!;
            public IClientConfig Config { get; set; } = null!;

            public Func<CancellationToken, Task<TransactionId>> BeginTransactionAsyncFunc { get; set; } = ct => Task.FromResult(new TransactionId("test-id"));
            public Func<BatchGetItemRequest, CancellationToken, Task<BatchGetItemResponse>> BatchGetItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new BatchGetItemResponse());
            public Func<BatchWriteItemRequest, CancellationToken, Task<BatchWriteItemResponse>> BatchWriteItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new BatchWriteItemResponse());
            public Func<DeleteItemRequest, CancellationToken, Task<DeleteItemResponse>> DeleteItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteItemResponse());
            public Func<GetItemRequest, CancellationToken, Task<GetItemResponse>> GetItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new GetItemResponse());
            public Func<PutItemRequest, CancellationToken, Task<PutItemResponse>> PutItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new PutItemResponse());
            public Func<QueryRequest, CancellationToken, Task<QueryResponse>> QueryAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new QueryResponse());
            public Func<ScanRequest, CancellationToken, Task<ScanResponse>> ScanAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ScanResponse());
            public Func<UpdateItemRequest, CancellationToken, Task<UpdateItemResponse>> UpdateItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateItemResponse());
            public Func<TransactWriteItemsRequest, CancellationToken, Task<TransactWriteItemsResponse>> TransactWriteItemsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new TransactWriteItemsResponse());
            public Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>> TransactGetItemsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new TransactGetItemsResponse());
            public Func<BatchExecuteStatementRequest, CancellationToken, Task<BatchExecuteStatementResponse>> BatchExecuteStatementAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new BatchExecuteStatementResponse());
            public Func<ExecuteStatementRequest, CancellationToken, Task<ExecuteStatementResponse>> ExecuteStatementAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ExecuteStatementResponse());
            public Func<ExecuteTransactionRequest, CancellationToken, Task<ExecuteTransactionResponse>> ExecuteTransactionAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ExecuteTransactionResponse());
            public Func<CreateBackupRequest, CancellationToken, Task<CreateBackupResponse>> CreateBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new CreateBackupResponse());
            public Func<CreateGlobalTableRequest, CancellationToken, Task<CreateGlobalTableResponse>> CreateGlobalTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new CreateGlobalTableResponse());
            public Func<CreateTableRequest, CancellationToken, Task<CreateTableResponse>> CreateTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new CreateTableResponse());
            public Func<DeleteBackupRequest, CancellationToken, Task<DeleteBackupResponse>> DeleteBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteBackupResponse());
            public Func<DeleteTableRequest, CancellationToken, Task<DeleteTableResponse>> DeleteTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteTableResponse());
            public Func<DescribeBackupRequest, CancellationToken, Task<DescribeBackupResponse>> DescribeBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeBackupResponse());
            public Func<DescribeContinuousBackupsRequest, CancellationToken, Task<DescribeContinuousBackupsResponse>> DescribeContinuousBackupsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeContinuousBackupsResponse());
            public Func<DescribeContributorInsightsRequest, CancellationToken, Task<DescribeContributorInsightsResponse>> DescribeContributorInsightsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeContributorInsightsResponse());
            public Func<DescribeEndpointsRequest, CancellationToken, Task<DescribeEndpointsResponse>> DescribeEndpointsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeEndpointsResponse());
            public Func<DescribeExportRequest, CancellationToken, Task<DescribeExportResponse>> DescribeExportAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeExportResponse());
            public Func<DescribeGlobalTableRequest, CancellationToken, Task<DescribeGlobalTableResponse>> DescribeGlobalTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeGlobalTableResponse());
            public Func<DescribeGlobalTableSettingsRequest, CancellationToken, Task<DescribeGlobalTableSettingsResponse>> DescribeGlobalTableSettingsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeGlobalTableSettingsResponse());
            public Func<DescribeImportRequest, CancellationToken, Task<DescribeImportResponse>> DescribeImportAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeImportResponse());
            public Func<DescribeKinesisStreamingDestinationRequest, CancellationToken, Task<DescribeKinesisStreamingDestinationResponse>> DescribeKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeKinesisStreamingDestinationResponse());
            public Func<DescribeLimitsRequest, CancellationToken, Task<DescribeLimitsResponse>> DescribeLimitsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeLimitsResponse());
            public Func<DescribeTableRequest, CancellationToken, Task<DescribeTableResponse>> DescribeTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeTableResponse());
            public Func<DescribeTableReplicaAutoScalingRequest, CancellationToken, Task<DescribeTableReplicaAutoScalingResponse>> DescribeTableReplicaAutoScalingAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeTableReplicaAutoScalingResponse());
            public Func<DescribeTimeToLiveRequest, CancellationToken, Task<DescribeTimeToLiveResponse>> DescribeTimeToLiveAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeTimeToLiveResponse());
            public Func<DisableKinesisStreamingDestinationRequest, CancellationToken, Task<DisableKinesisStreamingDestinationResponse>> DisableKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DisableKinesisStreamingDestinationResponse());
            public Func<EnableKinesisStreamingDestinationRequest, CancellationToken, Task<EnableKinesisStreamingDestinationResponse>> EnableKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new EnableKinesisStreamingDestinationResponse());
            public Func<ExportTableToPointInTimeRequest, CancellationToken, Task<ExportTableToPointInTimeResponse>> ExportTableToPointInTimeAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ExportTableToPointInTimeResponse());
            public Func<ImportTableRequest, CancellationToken, Task<ImportTableResponse>> ImportTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ImportTableResponse());
            public Func<ListBackupsRequest, CancellationToken, Task<ListBackupsResponse>> ListBackupsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListBackupsResponse());
            public Func<ListContributorInsightsRequest, CancellationToken, Task<ListContributorInsightsResponse>> ListContributorInsightsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListContributorInsightsResponse());
            public Func<ListExportsRequest, CancellationToken, Task<ListExportsResponse>> ListExportsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListExportsResponse());
            public Func<ListGlobalTablesRequest, CancellationToken, Task<ListGlobalTablesResponse>> ListGlobalTablesAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListGlobalTablesResponse());
            public Func<ListImportsRequest, CancellationToken, Task<ListImportsResponse>> ListImportsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListImportsResponse());
            public Func<ListTablesRequest, CancellationToken, Task<ListTablesResponse>> ListTablesAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListTablesResponse());
            public Func<ListTagsOfResourceRequest, CancellationToken, Task<ListTagsOfResourceResponse>> ListTagsOfResourceAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListTagsOfResourceResponse());
            public Func<DeleteResourcePolicyRequest, CancellationToken, Task<DeleteResourcePolicyResponse>> DeleteResourcePolicyAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteResourcePolicyResponse());
            public Func<GetResourcePolicyRequest, CancellationToken, Task<GetResourcePolicyResponse>> GetResourcePolicyAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new GetResourcePolicyResponse());
            public Func<PutResourcePolicyRequest, CancellationToken, Task<PutResourcePolicyResponse>> PutResourcePolicyAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new PutResourcePolicyResponse());
            public Func<RestoreTableFromBackupRequest, CancellationToken, Task<RestoreTableFromBackupResponse>> RestoreTableFromBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new RestoreTableFromBackupResponse());
            public Func<RestoreTableToPointInTimeRequest, CancellationToken, Task<RestoreTableToPointInTimeResponse>> RestoreTableToPointInTimeAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new RestoreTableToPointInTimeResponse());
            public Func<TagResourceRequest, CancellationToken, Task<TagResourceResponse>> TagResourceAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new TagResourceResponse());
            public Func<UntagResourceRequest, CancellationToken, Task<UntagResourceResponse>> UntagResourceAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UntagResourceResponse());
            public Func<UpdateContinuousBackupsRequest, CancellationToken, Task<UpdateContinuousBackupsResponse>> UpdateContinuousBackupsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateContinuousBackupsResponse());
            public Func<UpdateContributorInsightsRequest, CancellationToken, Task<UpdateContributorInsightsResponse>> UpdateContributorInsightsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateContributorInsightsResponse());
            public Func<UpdateGlobalTableRequest, CancellationToken, Task<UpdateGlobalTableResponse>> UpdateGlobalTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateGlobalTableResponse());
            public Func<UpdateGlobalTableSettingsRequest, CancellationToken, Task<UpdateGlobalTableSettingsResponse>> UpdateGlobalTableSettingsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateGlobalTableSettingsResponse());
            public Func<UpdateKinesisStreamingDestinationRequest, CancellationToken, Task<UpdateKinesisStreamingDestinationResponse>> UpdateKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateKinesisStreamingDestinationResponse());
            public Func<UpdateTableRequest, CancellationToken, Task<UpdateTableResponse>> UpdateTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateTableResponse());
            public Func<UpdateTableReplicaAutoScalingRequest, CancellationToken, Task<UpdateTableReplicaAutoScalingResponse>> UpdateTableReplicaAutoScalingAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateTableReplicaAutoScalingResponse());
            public Func<UpdateTimeToLiveRequest, CancellationToken, Task<UpdateTimeToLiveResponse>> UpdateTimeToLiveAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateTimeToLiveResponse());

            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default) => BatchGetItemAsyncFunc(new BatchGetItemRequest { RequestItems = requestItems, ReturnConsumedCapacity = returnConsumedCapacity }, cancellationToken);
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => BatchGetItemAsyncFunc(new BatchGetItemRequest { RequestItems = requestItems }, cancellationToken);
            public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default) => BatchGetItemAsyncFunc(request, cancellationToken);
            public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default) => BatchWriteItemAsyncFunc(new BatchWriteItemRequest { RequestItems = requestItems }, cancellationToken);
            public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default) => BatchWriteItemAsyncFunc(request, cancellationToken);
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => DeleteItemAsyncFunc(new DeleteItemRequest { TableName = tableName, Key = key }, cancellationToken);
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues, CancellationToken cancellationToken = default) => DeleteItemAsyncFunc(new DeleteItemRequest { TableName = tableName, Key = key, ReturnValues = returnValues }, cancellationToken);
            public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default) => DeleteItemAsyncFunc(request, cancellationToken);
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => GetItemAsyncFunc(new GetItemRequest { TableName = tableName, Key = key }, cancellationToken);
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default) => GetItemAsyncFunc(new GetItemRequest { TableName = tableName, Key = key, ConsistentRead = consistentRead }, cancellationToken);
            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default) => GetItemAsyncFunc(request, cancellationToken);
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default) => PutItemAsyncFunc(new PutItemRequest { TableName = tableName, Item = item }, cancellationToken);
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = default) => PutItemAsyncFunc(new PutItemRequest { TableName = tableName, Item = item, ReturnValues = returnValues }, cancellationToken);
            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default) => PutItemAsyncFunc(request, cancellationToken);
            public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default) => QueryAsyncFunc(request, cancellationToken);
            public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default) => ScanAsyncFunc(request, cancellationToken);
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => ScanAsyncFunc(new ScanRequest { TableName = tableName, AttributesToGet = attributesToGet }, cancellationToken);
            public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => ScanAsyncFunc(new ScanRequest { TableName = tableName, ScanFilter = scanFilter }, cancellationToken);
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => ScanAsyncFunc(new ScanRequest { TableName = tableName, AttributesToGet = attributesToGet, ScanFilter = scanFilter }, cancellationToken);
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default) => UpdateItemAsyncFunc(new UpdateItemRequest { TableName = tableName, Key = key, AttributeUpdates = attributeUpdates }, cancellationToken);
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default) => UpdateItemAsyncFunc(new UpdateItemRequest { TableName = tableName, Key = key, AttributeUpdates = attributeUpdates, ReturnValues = returnValues }, cancellationToken);
            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default) => UpdateItemAsyncFunc(request, cancellationToken);
            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => TransactWriteItemsAsyncFunc(request, cancellationToken);
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => TransactGetItemsAsyncFunc(request, cancellationToken);

            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => BatchExecuteStatementAsyncFunc(request, cancellationToken);
            public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => CreateBackupAsyncFunc(request, cancellationToken);
            public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => CreateGlobalTableAsyncFunc(request, cancellationToken);
            public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => CreateTableAsyncFunc(new CreateTableRequest { TableName = tableName, KeySchema = keySchema, AttributeDefinitions = attributeDefinitions, ProvisionedThroughput = provisionedThroughput }, cancellationToken);
            public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => CreateTableAsyncFunc(request, cancellationToken);
            public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => DeleteBackupAsyncFunc(request, cancellationToken);
            public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => DeleteResourcePolicyAsyncFunc(request, cancellationToken);
            public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => DeleteTableAsyncFunc(new DeleteTableRequest { TableName = tableName }, cancellationToken);
            public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => DeleteTableAsyncFunc(request, cancellationToken);
            public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => DescribeBackupAsyncFunc(request, cancellationToken);
            public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => DescribeContinuousBackupsAsyncFunc(request, cancellationToken);
            public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => DescribeContributorInsightsAsyncFunc(request, cancellationToken);
            public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => DescribeEndpointsAsyncFunc(request, cancellationToken);
            public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => DescribeExportAsyncFunc(request, cancellationToken);
            public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => DescribeGlobalTableAsyncFunc(request, cancellationToken);
            public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => DescribeGlobalTableSettingsAsyncFunc(request, cancellationToken);
            public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => DescribeImportAsyncFunc(request, cancellationToken);
            public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => DescribeKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => DescribeLimitsAsyncFunc(request, cancellationToken);
            public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => DescribeTableAsyncFunc(new DescribeTableRequest { TableName = tableName }, cancellationToken);
            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => DescribeTableAsyncFunc(request, cancellationToken);
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => DescribeTableReplicaAutoScalingAsyncFunc(request, cancellationToken);
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => DescribeTimeToLiveAsyncFunc(new DescribeTimeToLiveRequest { TableName = tableName }, cancellationToken);
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => DescribeTimeToLiveAsyncFunc(request, cancellationToken);
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => new Amazon.Runtime.Endpoints.Endpoint("https://dynamodb.us-east-1.amazonaws.com");
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => DisableKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public void Dispose() { }
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => EnableKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => ExecuteStatementAsyncFunc(request, cancellationToken);
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => ExecuteTransactionAsyncFunc(request, cancellationToken);
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => ExportTableToPointInTimeAsyncFunc(request, cancellationToken);
            public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => GetResourcePolicyAsyncFunc(request, cancellationToken);
            public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => ImportTableAsyncFunc(request, cancellationToken);
            public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => ListBackupsAsyncFunc(request, cancellationToken);
            public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => ListContributorInsightsAsyncFunc(request, cancellationToken);
            public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => ListExportsAsyncFunc(request, cancellationToken);
            public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => ListGlobalTablesAsyncFunc(request, cancellationToken);
            public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => ListImportsAsyncFunc(request, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest(), cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest { ExclusiveStartTableName = exclusiveStartTableName }, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest { ExclusiveStartTableName = exclusiveStartTableName, Limit = limit }, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest { Limit = limit }, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(request, cancellationToken);
            public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => ListTagsOfResourceAsyncFunc(request, cancellationToken);
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => PutResourcePolicyAsyncFunc(request, cancellationToken);
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => RestoreTableFromBackupAsyncFunc(request, cancellationToken);
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => RestoreTableToPointInTimeAsyncFunc(request, cancellationToken);
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => TagResourceAsyncFunc(request, cancellationToken);
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => UntagResourceAsyncFunc(request, cancellationToken);
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => UpdateContinuousBackupsAsyncFunc(request, cancellationToken);
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => UpdateContributorInsightsAsyncFunc(request, cancellationToken);
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => UpdateGlobalTableAsyncFunc(request, cancellationToken);
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => UpdateGlobalTableSettingsAsyncFunc(request, cancellationToken);
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => UpdateKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => UpdateTableAsyncFunc(new UpdateTableRequest { TableName = tableName, ProvisionedThroughput = provisionedThroughput }, cancellationToken);
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => UpdateTableAsyncFunc(request, cancellationToken);
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => UpdateTableReplicaAutoScalingAsyncFunc(request, cancellationToken);
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => UpdateTimeToLiveAsyncFunc(request, cancellationToken);
        }

        private sealed class MockTransactionStore : ITransactionStore
        {
            public Func<CancellationToken, Task<Transaction>> AddAsyncFunc { get; set; } = ct => Task.FromResult(Transaction.CreateNew());
            public Func<TransactionId, bool, CancellationToken, Task<Transaction>> GetAsyncFunc { get; set; } = (id, consistent, ct) => Task.FromResult(Transaction.CreateNew());
            public Func<Transaction, CancellationToken, Task<Transaction>> UpdateAsyncFunc { get; set; } = (txn, ct) => Task.FromResult(txn);
            public Func<Transaction, AmazonDynamoDBRequest, CancellationToken, Task<Transaction>> AppendRequestAsyncFunc { get; set; } = (txn, req, ct) => Task.FromResult(txn);
            public Func<int, CancellationToken, Task<ImmutableList<Transaction>>> ListAsyncFunc { get; set; } = (limit, ct) => Task.FromResult(ImmutableList<Transaction>.Empty);
            public Func<TransactionId, TimeSpan, CancellationToken, Task<bool>> TryRemoveAsyncFunc { get; set; } = (id, duration, ct) => Task.FromResult(true);
            public Func<TransactionId, Dictionary<string, AttributeValue>> GetKeyFunc { get; set; } = id => new Dictionary<string, AttributeValue>();
            public Func<TransactionId, CancellationToken, Task<bool>> ContainsAsyncFunc { get; set; } = (id, ct) => Task.FromResult(false);
            public Func<TransactionId, CancellationToken, Task> RemoveAsyncFunc { get; set; } = (id, ct) => Task.CompletedTask;

            public Task AddAsync(Transaction transaction, CancellationToken cancellationToken) => AddAsyncFunc(cancellationToken);
            public Task<Transaction> GetAsync(TransactionId id, bool consistentRead, CancellationToken cancellationToken) => GetAsyncFunc(id, consistentRead, cancellationToken);
            public Task<Transaction> UpdateAsync(Transaction transaction, CancellationToken cancellationToken) => UpdateAsyncFunc(transaction, cancellationToken);
            public Task<Transaction> AppendRequestAsync(Transaction transaction, AmazonDynamoDBRequest request, CancellationToken cancellationToken) => AppendRequestAsyncFunc(transaction, request, cancellationToken);
            public Task<ImmutableList<Transaction>> ListAsync(int limit, CancellationToken cancellationToken) => ListAsyncFunc(limit, cancellationToken);
            public Task<bool> TryRemoveAsync(TransactionId id, TimeSpan deleteAfterDuration, CancellationToken cancellationToken) => TryRemoveAsyncFunc(id, deleteAfterDuration, cancellationToken);
            public Dictionary<string, AttributeValue> GetKey(TransactionId id) => GetKeyFunc(id);
            public Task<bool> ContainsAsync(TransactionId id, CancellationToken cancellationToken) => ContainsAsyncFunc(id, cancellationToken);
            public Task RemoveAsync(TransactionId id, CancellationToken cancellationToken) => RemoveAsyncFunc(id, cancellationToken);
        }

        private sealed class MockIsolatedGetItemService<T> : IIsolatedGetItemService<T>
            where T : IsolationLevelServiceType
        {
            public Func<GetItemRequest, CancellationToken, Task<GetItemResponse>> GetItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new GetItemResponse());
            public Func<BatchGetItemRequest, CancellationToken, Task<BatchGetItemResponse>> BatchGetItemAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new BatchGetItemResponse());
            public Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>> TransactGetItemsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new TransactGetItemsResponse());

            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken) => GetItemAsyncFunc(request, cancellationToken);
            public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken) => BatchGetItemAsyncFunc(request, cancellationToken);
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken) => TransactGetItemsAsyncFunc(request, cancellationToken);
        }

        private sealed class MockVersionedItemStore : IVersionedItemStore
        {
            public Func<Transaction, AmazonDynamoDBRequest, CancellationToken, Task<ImmutableDictionary<ItemKey, ItemTransactionState>>> AcquireLocksAsyncFunc { get; set; } = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty);
            public Func<TransactionId, TransactionId, ImmutableList<ItemKey>, bool, ImmutableDictionary<ItemKey, ItemTransactionState>, ImmutableDictionary<ItemKey, ItemRecord>, CancellationToken, Task> ReleaseLocksAsyncFunc { get; set; } = (id1, id2, keys, rollback, states, records, ct) => Task.CompletedTask;
            public Func<Transaction, bool, ImmutableDictionary<ItemKey, ItemRecord>, CancellationToken, Task> ReleaseLocksAsync2Func { get; set; } = (txn, rollback, records, ct) => Task.CompletedTask;
            public Func<AmazonDynamoDBRequest, CancellationToken, Task<ImmutableList<ItemRecord>>> GetItemsToBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(ImmutableList<ItemRecord>.Empty);
            public Func<ApplyRequestRequest, CancellationToken, Task<AmazonWebServiceResponse>> ApplyRequestAsyncFunc { get; set; } = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new GetItemResponse());
            public Func<ItemKey, Dictionary<string, AttributeValue>, ItemResponseAndTransactionState<ItemRecord>> GetItemRecordAndTransactionStateFunc { get; set; } = (key, item) => new ItemResponseAndTransactionState<ItemRecord>(new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty), new TransactionStateValue(false, null, null, false, false));
            public Func<Dictionary<string, AttributeValue>, Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>> GetItemRecordAndTransactionState2Func { get; set; } = item => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(item, new TransactionStateValue(false, null, null, false, false));

            public Task<ImmutableDictionary<ItemKey, ItemTransactionState>> AcquireLocksAsync(Transaction transaction, AmazonDynamoDBRequest request, CancellationToken cancellationToken) => AcquireLocksAsyncFunc(transaction, request, cancellationToken);
            public Task ReleaseLocksAsync(TransactionId transactionId, TransactionId owningTransactionId, ImmutableList<ItemKey> itemKeys, bool rollback, ImmutableDictionary<ItemKey, ItemTransactionState> itemTransactionStatesByKey, ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey, CancellationToken cancellationToken) => ReleaseLocksAsyncFunc(transactionId, owningTransactionId, itemKeys, rollback, itemTransactionStatesByKey, rollbackImagesByKey, cancellationToken);
            public Task ReleaseLocksAsync(Transaction transaction, bool rollback, ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey, CancellationToken cancellationToken) => ReleaseLocksAsync2Func(transaction, rollback, rollbackImagesByKey, cancellationToken);
            public Task<ImmutableList<ItemRecord>> GetItemsToBackupAsync(AmazonDynamoDBRequest request, CancellationToken cancellationToken) => GetItemsToBackupAsyncFunc(request, cancellationToken);
            public Task<AmazonWebServiceResponse> ApplyRequestAsync(ApplyRequestRequest request, CancellationToken cancellationToken) => ApplyRequestAsyncFunc(request, cancellationToken);
            public ItemResponseAndTransactionState<ItemRecord> GetItemRecordAndTransactionState(ItemKey itemKey, Dictionary<string, AttributeValue> item) => GetItemRecordAndTransactionStateFunc(itemKey, item);
            public Tuple<Dictionary<string, AttributeValue>, TransactionStateValue> GetItemRecordAndTransactionState(Dictionary<string, AttributeValue> item) => GetItemRecordAndTransactionState2Func(item);
        }

        private sealed class MockItemImageStore : IItemImageStore
        {
            public Func<TransactionVersion, ImmutableList<ItemRecord>, CancellationToken, Task> AddItemImagesAsyncFunc { get; set; } = (version, records, ct) => Task.CompletedTask;
            public Func<TransactionVersion, CancellationToken, Task<ImmutableList<ItemRecord>>> GetItemImagesAsyncFunc { get; set; } = (version, ct) => Task.FromResult(ImmutableList<ItemRecord>.Empty);
            public Func<Transaction, CancellationToken, Task> DeleteItemImagesAsync1Func { get; set; } = (txn, ct) => Task.CompletedTask;
            public Func<TransactionVersion, CancellationToken, Task> DeleteItemImagesAsync2Func { get; set; } = (version, ct) => Task.CompletedTask;
            public Func<TransactionVersion, Dictionary<string, AttributeValue>> GetKeyFunc { get; set; } = version => new Dictionary<string, AttributeValue>();
            public Func<TransactionId, CancellationToken, Task<ImmutableList<TransactionVersion>>> GetTransactionVersionsFunc { get; set; } = (id, ct) => Task.FromResult(ImmutableList<TransactionVersion>.Empty);

            public Task AddItemImagesAsync(TransactionVersion transactionVersion, ImmutableList<ItemRecord> itemsToBackup, CancellationToken cancellationToken) => AddItemImagesAsyncFunc(transactionVersion, itemsToBackup, cancellationToken);
            public Task<ImmutableList<ItemRecord>> GetItemImagesAsync(TransactionVersion transactionVersion, CancellationToken cancellationToken) => GetItemImagesAsyncFunc(transactionVersion, cancellationToken);
            public Task DeleteItemImagesAsync(Transaction transaction, CancellationToken cancellationToken) => DeleteItemImagesAsync1Func(transaction, cancellationToken);
            public Task DeleteItemImagesAsync(TransactionVersion transactionVersion, CancellationToken cancellationToken) => DeleteItemImagesAsync2Func(transactionVersion, cancellationToken);
            public Dictionary<string, AttributeValue> GetKey(TransactionVersion transactionVersion) => GetKeyFunc(transactionVersion);
            public Task<ImmutableList<TransactionVersion>> GetTransactionVersions(TransactionId id, CancellationToken cancellationToken) => GetTransactionVersionsFunc(id, cancellationToken);
        }

        private sealed class MockRequestService : IRequestService
        {
            public Func<AmazonDynamoDBRequest, CancellationToken, Task<ImmutableList<ItemRequestDetail>>> GetItemRequestDetailsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(ImmutableList<ItemRequestDetail>.Empty);
            public Func<Transaction, CancellationToken, Task<ImmutableList<LockedItemRequestAction>>> GetItemRequestActionsAsyncFunc { get; set; } = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty);

            public Task<ImmutableList<ItemRequestDetail>> GetItemRequestDetailsAsync(AmazonDynamoDBRequest request, CancellationToken cancellationToken) => GetItemRequestDetailsAsyncFunc(request, cancellationToken);
            public Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(Transaction transaction, CancellationToken cancellationToken) => GetItemRequestActionsAsyncFunc(transaction, cancellationToken);
        }

        private sealed class MockTransactionServiceEvents : ITransactionServiceEvents
        {
            public Func<TransactionVersion, CancellationToken, Task>? OnUpdateFullyAppliedRequestsBeginAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task<bool>>? OnDoCommitBeginAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnDoRollbackBeginAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnResumeTransactionFinishAsync { get; set; }
            public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnApplyRequestAsync { get; set; }
            public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnAcquireLockAsync { get; set; }
            public Func<TransactionId, bool, CancellationToken, Task>? OnReleaseLocksAsync { get; set; }
            public Func<TransactionId, TransactionId, CancellationToken, Task>? OnReleaseLockFromOtherTransactionAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnBackupItemImagesAsync { get; set; }
        }

        private sealed class MockFullyAppliedRequestService : IFullyAppliedRequestService
        {
            public Func<TransactionVersion, CancellationToken, Task<bool>> IsFullyAppliedAsyncFunc { get; set; } = (version, ct) => Task.FromResult(true);
            public Func<TransactionVersion, CancellationToken, Task> SetFullyAppliedAsyncFunc { get; set; } = (version, ct) => Task.CompletedTask;

            public Task<bool> IsFullyAppliedAsync(TransactionVersion transactionVersion, CancellationToken cancellationToken) => IsFullyAppliedAsyncFunc(transactionVersion, cancellationToken);
            public Task SetFullyAppliedAsync(TransactionVersion transactionVersion, CancellationToken cancellationToken) => SetFullyAppliedAsyncFunc(transactionVersion, cancellationToken);
        }

        private sealed class MockOptionsSnapshot<T> : IOptionsSnapshot<T> where T : class, new()
        {
            public MockOptionsSnapshot(T value)
            {
                Value = value;
            }

            public T Value { get; }
            public T Get(string? name) => Value;
        }

        private static string Combine(params string?[] expressions)
        {
            return string.Join(" AND ", expressions.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e?.Trim()));
        }

        private static bool IsSupportedConditionExpression(ConditionCheck conditionCheck, string conditionExpressionFunction)
        {
            return conditionCheck.Key.Keys.Any(key => conditionCheck.ConditionExpression == $"{conditionExpressionFunction}({key})");
        }
    }
}
