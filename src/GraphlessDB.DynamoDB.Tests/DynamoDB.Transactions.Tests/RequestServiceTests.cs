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
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class RequestServiceTests
    {
        private sealed class MockAmazonDynamoDBKeyService : IAmazonDynamoDBKeyService
        {
            public Func<string, ImmutableDictionary<string, AttributeValue>, CancellationToken, Task<ImmutableDictionary<string, AttributeValue>>> CreateKeyMapAsyncFunc { get; set; } =
                (tableName, item, ct) => Task.FromResult(ImmutableDictionary<string, AttributeValue>.Empty);

            public Task<ImmutableDictionary<string, AttributeValue>> CreateKeyMapAsync(
                string tableName,
                ImmutableDictionary<string, AttributeValue> item,
                CancellationToken cancellationToken)
            {
                return CreateKeyMapAsyncFunc(tableName, item, cancellationToken);
            }
        }

        private static RequestService CreateService(MockAmazonDynamoDBKeyService? keyService = null)
        {
            return new RequestService(keyService ?? new MockAmazonDynamoDBKeyService());
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailForGetItemRequest()
        {
            var service = CreateService();
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("test-id") }
                },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#name", "Name" }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Get, result[0].RequestAction);
            Assert.IsNull(result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(0, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailForPutItemRequest()
        {
            var keyMap = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", AttributeValueFactory.CreateS("test-id"));

            var mockKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) => Task.FromResult(keyMap)
            };

            var service = CreateService(mockKeyService);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("test-id") },
                    { "Data", AttributeValueFactory.CreateS("test-data") }
                },
                ConditionExpression = "attribute_not_exists(Id)",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#name", "Name" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":val", AttributeValueFactory.CreateS("value") }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Put, result[0].RequestAction);
            Assert.AreEqual("attribute_not_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailForDeleteItemRequest()
        {
            var service = CreateService();
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("test-id") }
                },
                ConditionExpression = "attribute_exists(Id)",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#name", "Name" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":val", AttributeValueFactory.CreateS("value") }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Delete, result[0].RequestAction);
            Assert.AreEqual("attribute_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailForUpdateItemRequest()
        {
            var service = CreateService();
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("test-id") }
                },
                UpdateExpression = "SET #name = :val",
                ConditionExpression = "attribute_exists(Id)",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#name", "Name" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":val", AttributeValueFactory.CreateS("value") }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Update, result[0].RequestAction);
            Assert.AreEqual("attribute_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailsForTransactGetItemsRequest()
        {
            var service = CreateService();
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "Table1",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("id1") }
                            },
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#name", "Name" }
                            }
                        }
                    },
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "Table2",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("id2") }
                            },
                            ExpressionAttributeNames = new Dictionary<string, string>()
                        }
                    }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("Table1", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Get, result[0].RequestAction);
            Assert.IsNull(result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual(0, result[0].ExpressionAttributeValues.Count);

            Assert.AreEqual("Table2", result[1].Key.TableName);
            Assert.AreEqual(RequestAction.Get, result[1].RequestAction);
            Assert.IsNull(result[1].ConditionExpression);
            Assert.AreEqual(0, result[1].ExpressionAttributeNames.Count);
            Assert.AreEqual(0, result[1].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailsForTransactWriteItemsRequestWithPut()
        {
            var keyMap = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", AttributeValueFactory.CreateS("test-id"));

            var mockKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) => Task.FromResult(keyMap)
            };

            var service = CreateService(mockKeyService);
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
                                { "Id", AttributeValueFactory.CreateS("test-id") },
                                { "Data", AttributeValueFactory.CreateS("test-data") }
                            },
                            ConditionExpression = "attribute_not_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#name", "Name" }
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":val", AttributeValueFactory.CreateS("value") }
                            }
                        }
                    }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Put, result[0].RequestAction);
            Assert.AreEqual("attribute_not_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailsForTransactWriteItemsRequestWithUpdate()
        {
            var service = CreateService();
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            },
                            UpdateExpression = "SET #name = :val",
                            ConditionExpression = "attribute_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#name", "Name" }
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":val", AttributeValueFactory.CreateS("value") }
                            }
                        }
                    }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Update, result[0].RequestAction);
            Assert.AreEqual("attribute_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailsForTransactWriteItemsRequestWithDelete()
        {
            var service = CreateService();
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            },
                            ConditionExpression = "attribute_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#name", "Name" }
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":val", AttributeValueFactory.CreateS("value") }
                            }
                        }
                    }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.Delete, result[0].RequestAction);
            Assert.AreEqual("attribute_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncReturnsItemRequestDetailsForTransactWriteItemsRequestWithConditionCheck()
        {
            var service = CreateService();
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            },
                            ConditionExpression = "attribute_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#name", "Name" }
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":val", AttributeValueFactory.CreateS("value") }
                            }
                        }
                    }
                }
            };

            var result = await service.GetItemRequestDetailsAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(RequestAction.ConditionCheck, result[0].RequestAction);
            Assert.AreEqual("attribute_exists(Id)", result[0].ConditionExpression);
            Assert.AreEqual(1, result[0].ExpressionAttributeNames.Count);
            Assert.AreEqual("Name", result[0].ExpressionAttributeNames["#name"]);
            Assert.AreEqual(1, result[0].ExpressionAttributeValues.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncThrowsNotSupportedExceptionForUnsupportedRequest()
        {
            var service = CreateService();
            var request = new QueryRequest(); // Unsupported request type

            await Assert.ThrowsExceptionAsync<NotSupportedException>(
                async () => await service.GetItemRequestDetailsAsync(request, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetItemRequestActionsAsyncReturnsActionsForSingleRequest()
        {
            var service = CreateService();
            var transaction = new Transaction(
                "txn-1",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty.Add(
                    new RequestRecord(
                        1,
                        new GetItemRequest
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            }
                        },
                        null,
                        null,
                        null,
                        null,
                        null)));

            var result = await service.GetItemRequestActionsAsync(transaction, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(1, result[0].RequestId);
            Assert.AreEqual(RequestAction.Get, result[0].RequestAction);
        }

        [TestMethod]
        public async Task GetItemRequestActionsAsyncReturnsActionsForMultipleRequests()
        {
            var mockKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) =>
                {
                    // Return different keys for different items
                    if (item.TryGetValue("Id", out var idValue))
                    {
                        return Task.FromResult(ImmutableDictionary<string, AttributeValue>.Empty
                            .Add("Id", idValue));
                    }
                    return Task.FromResult(ImmutableDictionary<string, AttributeValue>.Empty);
                }
            };

            var service = CreateService(mockKeyService);
            var transaction = new Transaction(
                "txn-1",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty
                    .Add(new RequestRecord(
                        1,
                        new GetItemRequest
                        {
                            TableName = "Table1",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("id1") }
                            }
                        },
                        null,
                        null,
                        null,
                        null,
                        null))
                    .Add(new RequestRecord(
                        2,
                        null,
                        new PutItemRequest
                        {
                            TableName = "Table2",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("id2") },
                                { "Data", AttributeValueFactory.CreateS("data2") }
                            }
                        },
                        null,
                        null,
                        null,
                        null)));

            var result = await service.GetItemRequestActionsAsync(transaction, CancellationToken.None);

            Assert.AreEqual(2, result.Count);

            // Find the items by table name (order is not guaranteed)
            var table1Item = result.FirstOrDefault(r => r.Key.TableName == "Table1");
            var table2Item = result.FirstOrDefault(r => r.Key.TableName == "Table2");

            Assert.IsNotNull(table1Item);
            Assert.AreEqual(1, table1Item.RequestId);
            Assert.AreEqual(RequestAction.Get, table1Item.RequestAction);

            Assert.IsNotNull(table2Item);
            Assert.AreEqual(2, table2Item.RequestId);
            Assert.AreEqual(RequestAction.Put, table2Item.RequestAction);
        }

        [TestMethod]
        public async Task GetItemRequestActionsAsyncKeepsFirstActionWhenMultipleGetRequests()
        {
            var service = CreateService();
            var transaction = new Transaction(
                "txn-1",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty
                    .Add(new RequestRecord(
                        1,
                        new GetItemRequest
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            }
                        },
                        null,
                        null,
                        null,
                        null,
                        null))
                    .Add(new RequestRecord(
                        2,
                        new GetItemRequest
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            }
                        },
                        null,
                        null,
                        null,
                        null,
                        null)));

            var result = await service.GetItemRequestActionsAsync(transaction, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(1, result[0].RequestId);
            Assert.AreEqual(RequestAction.Get, result[0].RequestAction);
        }

        [TestMethod]
        public async Task GetItemRequestActionsAsyncUpgradesGetLockToWriteLock()
        {
            var keyMap = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", AttributeValueFactory.CreateS("test-id"));

            var mockKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) => Task.FromResult(keyMap)
            };

            var service = CreateService(mockKeyService);
            var transaction = new Transaction(
                "txn-1",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty
                    .Add(new RequestRecord(
                        1,
                        new GetItemRequest
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            }
                        },
                        null,
                        null,
                        null,
                        null,
                        null))
                    .Add(new RequestRecord(
                        2,
                        null,
                        new PutItemRequest
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") },
                                { "Data", AttributeValueFactory.CreateS("new-data") }
                            }
                        },
                        null,
                        null,
                        null,
                        null)));

            var result = await service.GetItemRequestActionsAsync(transaction, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("TestTable", result[0].Key.TableName);
            Assert.AreEqual(2, result[0].RequestId);
            Assert.AreEqual(RequestAction.Put, result[0].RequestAction);
        }

        [TestMethod]
        public async Task GetItemRequestActionsAsyncThrowsInvalidOperationExceptionForMultipleWriteRequests()
        {
            var keyMap = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", AttributeValueFactory.CreateS("test-id"));

            var mockKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) => Task.FromResult(keyMap)
            };

            var service = CreateService(mockKeyService);
            var transaction = new Transaction(
                "txn-1",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty
                    .Add(new RequestRecord(
                        1,
                        null,
                        new PutItemRequest
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") },
                                { "Data", AttributeValueFactory.CreateS("data1") }
                            }
                        },
                        null,
                        null,
                        null,
                        null))
                    .Add(new RequestRecord(
                        2,
                        null,
                        null,
                        new UpdateItemRequest
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("test-id") }
                            },
                            UpdateExpression = "SET #data = :val"
                        },
                        null,
                        null,
                        null)));

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                async () => await service.GetItemRequestActionsAsync(transaction, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetItemRequestActionsAsyncHandlesEmptyTransaction()
        {
            var service = CreateService();
            var transaction = new Transaction(
                "txn-1",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty);

            var result = await service.GetItemRequestActionsAsync(transaction, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemRequestDetailsAsyncThrowsNotSupportedExceptionForTransactWriteItemWithNoAction()
        {
            var service = CreateService();
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem()
                }
            };

            await Assert.ThrowsExceptionAsync<NotSupportedException>(
                async () => await service.GetItemRequestDetailsAsync(request, CancellationToken.None));
        }

        [TestMethod]
        public void GetRequestActionThrowsNotSupportedExceptionForEmptyTransactWriteItem()
        {
            var requestServiceType = typeof(RequestService);
            var method = requestServiceType.GetMethod("GetRequestAction", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Assert.IsNotNull(method);

            var emptyItem = new TransactWriteItem();
            Assert.ThrowsException<System.Reflection.TargetInvocationException>(() =>
            {
                method.Invoke(null, new object[] { emptyItem });
            });
        }

        [TestMethod]
        public void GetConditionExpressionThrowsNotSupportedExceptionForEmptyTransactWriteItem()
        {
            var requestServiceType = typeof(RequestService);
            var method = requestServiceType.GetMethod("GetConditionExpression", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Assert.IsNotNull(method);

            var emptyItem = new TransactWriteItem();
            Assert.ThrowsException<System.Reflection.TargetInvocationException>(() =>
            {
                method.Invoke(null, new object[] { emptyItem });
            });
        }

        [TestMethod]
        public void GetExpressionAttributeNamesThrowsNotSupportedExceptionForEmptyTransactWriteItem()
        {
            var requestServiceType = typeof(RequestService);
            var method = requestServiceType.GetMethod("GetExpressionAttributeNames", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Assert.IsNotNull(method);

            var emptyItem = new TransactWriteItem();
            Assert.ThrowsException<System.Reflection.TargetInvocationException>(() =>
            {
                method.Invoke(null, new object[] { emptyItem });
            });
        }

        [TestMethod]
        public void GetExpressionAttributeValuesThrowsNotSupportedExceptionForEmptyTransactWriteItem()
        {
            var requestServiceType = typeof(RequestService);
            var method = requestServiceType.GetMethod("GetExpressionAttributeValues", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Assert.IsNotNull(method);

            var emptyItem = new TransactWriteItem();
            Assert.ThrowsException<System.Reflection.TargetInvocationException>(() =>
            {
                method.Invoke(null, new object[] { emptyItem });
            });
        }
    }
}
