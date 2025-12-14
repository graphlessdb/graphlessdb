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
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Internal.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBWithTransactionsValidateAppendRequestTests
    {
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicMethods | DynamicallyAccessedMemberTypes.PublicConstructors)]
        private static readonly Type AmazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);

        private sealed class MockRequestService : IRequestService
        {
            public Func<AmazonDynamoDBRequest, CancellationToken, Task<ImmutableList<ItemRequestDetail>>> GetItemRequestDetailsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(ImmutableList<ItemRequestDetail>.Empty);
            public Func<Transaction, CancellationToken, Task<ImmutableList<LockedItemRequestAction>>> GetItemRequestActionsAsyncFunc { get; set; } = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty);

            public Task<ImmutableList<ItemRequestDetail>> GetItemRequestDetailsAsync(AmazonDynamoDBRequest request, CancellationToken cancellationToken) => GetItemRequestDetailsAsyncFunc(request, cancellationToken);
            public Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(Transaction transaction, CancellationToken cancellationToken) => GetItemRequestActionsAsyncFunc(transaction, cancellationToken);
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

        private static AmazonDynamoDBWithTransactions CreateServiceWithRequestService(MockRequestService requestService)
        {
            var options = new AmazonDynamoDBOptions
            {
                TransactionStaleDuration = TimeSpan.FromMinutes(5),
                QuickTransactionsEnabled = false,
                TransactWriteItemCountMaxValue = 100,
                TransactGetItemCountMaxValue = 100
            };

            var optionsSnapshot = new MockOptionsSnapshot<AmazonDynamoDBOptions>(options);

            var constructor = AmazonDynamoDBWithTransactionsType.GetConstructor(
                BindingFlags.Public | BindingFlags.Instance,
                null,
                new[]
                {
                    typeof(IOptionsSnapshot<AmazonDynamoDBOptions>),
                    typeof(IAmazonDynamoDB),
                    typeof(IIsolatedGetItemService<UnCommittedIsolationLevelServiceType>),
                    typeof(IIsolatedGetItemService<CommittedIsolationLevelServiceType>),
                    typeof(ITransactionStore),
                    typeof(IVersionedItemStore),
                    typeof(IItemImageStore),
                    typeof(IRequestService),
                    typeof(ITransactionServiceEvents),
                    typeof(IFullyAppliedRequestService)
                },
                null);

            Assert.IsNotNull(constructor, "Constructor not found");

            var instance = constructor.Invoke(new object?[]
            {
                optionsSnapshot,
                null!,
                null!,
                null!,
                null!,
                null!,
                null!,
                requestService,
                null!,
                null!
            });

            return (AmazonDynamoDBWithTransactions)instance;
        }

        private static async Task CallValidateAppendRequestAsync(
            AmazonDynamoDBWithTransactions service,
            Transaction transaction,
            AmazonDynamoDBRequest request)
        {
            var method = AmazonDynamoDBWithTransactionsType.GetMethod(
                "ValidateAppendRequestAsync",
                BindingFlags.NonPublic | BindingFlags.Instance);

            Assert.IsNotNull(method, "ValidateAppendRequestAsync method not found");

            var task = (Task)method.Invoke(service, new object[] { transaction, request, CancellationToken.None })!;
            await task;
        }

        private static ItemKey CreateItemKey(string tableName, string partitionKeyValue)
        {
            var key = new Dictionary<string, AttributeValue>
            {
                { "id", new AttributeValue { S = partitionKeyValue } }
            }.ToImmutableDictionary();

            return ItemKey.Create(tableName, key);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncMultipleGetRequestsToSameItemSucceeds()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            var getDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Get,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(ImmutableList.Create(getDetail));
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingGetRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingGetRequest)));

            var newGetRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newGetRequest);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncUpgradeGetToWriteRequestSucceeds()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            var getDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Get,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(getDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(putDetail));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingGetRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingGetRequest)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newPutRequest);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncInvalidPreviousRequestPatternThrowsException()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            
            var putDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            
            var updateDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Update,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetail));
                    }
                    else if (callCount == 2)
                    {
                        return Task.FromResult(ImmutableList.Create(updateDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(putDetail));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            var existingUpdateRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(
                    RequestRecord.Create(0, existingPutRequest),
                    RequestRecord.Create(1, existingUpdateRequest)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() =>
                CallValidateAppendRequestAsync(service, transaction, newPutRequest));
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncNewGetRequestAlwaysSucceeds()
        {
            var itemKeyA = CreateItemKey("TestTable", "item-a");
            var putDetail = new ItemRequestDetail(
                itemKeyA,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var getDetail = new ItemRequestDetail(
                itemKeyA,
                RequestAction.Get,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(getDetail));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingPutRequest)));

            var newGetRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newGetRequest);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncWriteRequestForNewItemSucceeds()
        {
            var itemKeyA = CreateItemKey("TestTable", "item-a");
            var itemKeyC = CreateItemKey("TestTable", "item-c");
            
            var putDetailA = new ItemRequestDetail(
                itemKeyA,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetailC = new ItemRequestDetail(
                itemKeyC,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailA));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailC));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingPutRequest)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-c" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newPutRequest);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncDuplicateWriteRequestsThrowsDuplicateRequestException()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            var putDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(ImmutableList.Create(putDetail));
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingPutRequest)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await Assert.ThrowsExceptionAsync<DuplicateRequestException>(() =>
                CallValidateAppendRequestAsync(service, transaction, newPutRequest));
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncWriteAfterWriteThrowsDuplicateRequestException()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            var updateDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Update,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var deleteDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Delete,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(updateDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(deleteDetail));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingUpdateRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingUpdateRequest)));

            var newDeleteRequest = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await Assert.ThrowsExceptionAsync<DuplicateRequestException>(() =>
                CallValidateAppendRequestAsync(service, transaction, newDeleteRequest));
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncPutAfterDeleteThrowsDuplicateRequestException()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            var deleteDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Delete,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(deleteDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(putDetail));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingDeleteRequest = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingDeleteRequest)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await Assert.ThrowsExceptionAsync<DuplicateRequestException>(() =>
                CallValidateAppendRequestAsync(service, transaction, newPutRequest));
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncComplexTransactionWithMultipleItemsValidatesCorrectly()
        {
            var itemKeyA = CreateItemKey("TestTable", "item-a");
            var itemKeyB = CreateItemKey("TestTable", "item-b");
            var itemKeyC = CreateItemKey("TestTable", "item-c");

            var getDetail = new ItemRequestDetail(
                itemKeyA,
                RequestAction.Get,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetailB = new ItemRequestDetail(
                itemKeyB,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var updateDetail = new ItemRequestDetail(
                itemKeyC,
                RequestAction.Update,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetailA = new ItemRequestDetail(
                itemKeyA,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(getDetail));
                    }
                    else if (callCount == 2)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailB));
                    }
                    else if (callCount == 3)
                    {
                        return Task.FromResult(ImmutableList.Create(updateDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailA));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingGetRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            var existingPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-b" } } }
            };
            var existingUpdateRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-c" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(
                    RequestRecord.Create(0, existingGetRequest),
                    RequestRecord.Create(1, existingPutRequest),
                    RequestRecord.Create(2, existingUpdateRequest)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newPutRequest);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncMultipleRequestsForDifferentItemsSucceeds()
        {
            var itemKeyA = CreateItemKey("TestTable", "item-a");
            var itemKeyB = CreateItemKey("TestTable", "item-b");
            var itemKeyC = CreateItemKey("TestTable", "item-c");
            var itemKeyD = CreateItemKey("TestTable", "item-d");

            var putDetailA = new ItemRequestDetail(
                itemKeyA,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetailB = new ItemRequestDetail(
                itemKeyB,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetailC = new ItemRequestDetail(
                itemKeyC,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var putDetailD = new ItemRequestDetail(
                itemKeyD,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailA));
                    }
                    else if (callCount == 2)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailB));
                    }
                    else if (callCount == 3)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailC));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(putDetailD));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingPutRequestA = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            var existingPutRequestB = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-b" } } }
            };
            var existingPutRequestC = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-c" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(
                    RequestRecord.Create(0, existingPutRequestA),
                    RequestRecord.Create(1, existingPutRequestB),
                    RequestRecord.Create(2, existingPutRequestC)));

            var newPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-d" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newPutRequest);
        }

        [TestMethod]
        public async Task ValidateAppendRequestAsyncGetAfterWriteSucceeds()
        {
            var itemKey = CreateItemKey("TestTable", "item-a");
            var putDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var getDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Get,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var callCount = 0;
            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(ImmutableList.Create(putDetail));
                    }
                    else
                    {
                        return Task.FromResult(ImmutableList.Create(getDetail));
                    }
                }
            };

            var service = CreateServiceWithRequestService(requestService);
            
            var existingPutRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };
            
            var transaction = new Transaction(
                "test-id",
                TransactionState.Active,
                1,
                DateTime.UtcNow,
                ImmutableList.Create(RequestRecord.Create(0, existingPutRequest)));

            var newGetRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "id", new AttributeValue { S = "item-a" } } }
            };

            await CallValidateAppendRequestAsync(service, transaction, newGetRequest);
        }
    }
}
