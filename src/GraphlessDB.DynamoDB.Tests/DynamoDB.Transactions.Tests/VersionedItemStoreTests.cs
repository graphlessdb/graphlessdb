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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.Collections.Immutable;
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class VersionedItemStoreTests
    {
        private sealed class MockRequestService : IRequestService
        {
            public Func<Transaction, CancellationToken, Task<ImmutableList<LockedItemRequestAction>>> GetItemRequestActionsAsyncFunc { get; set; } = (_, _) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty);
            public Func<AmazonDynamoDBRequest, CancellationToken, Task<ImmutableList<ItemRequestDetail>>> GetItemRequestDetailsAsyncFunc { get; set; } = (_, _) => Task.FromResult(ImmutableList<ItemRequestDetail>.Empty);

            public Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(Transaction transaction, CancellationToken cancellationToken)
            {
                return GetItemRequestActionsAsyncFunc(transaction, cancellationToken);
            }

            public Task<ImmutableList<ItemRequestDetail>> GetItemRequestDetailsAsync(AmazonDynamoDBRequest request, CancellationToken cancellationToken)
            {
                return GetItemRequestDetailsAsyncFunc(request, cancellationToken);
            }
        }

        private sealed class MockTransactionServiceEvents : ITransactionServiceEvents
        {
            public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnAcquireLockAsync { get; set; }
            public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnApplyRequestAsync { get; set; }
            public Func<TransactionId, bool, CancellationToken, Task>? OnReleaseLocksAsync { get; set; }
            public Func<TransactionId, TransactionId, CancellationToken, Task>? OnReleaseLockFromOtherTransactionAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnResumeTransactionFinishAsync { get; set; }
            public Func<TransactionVersion, CancellationToken, Task>? OnUpdateFullyAppliedRequestsBeginAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnBackupItemImagesAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task<bool>>? OnDoCommitBeginAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnDoRollbackBeginAsync { get; set; }
        }

        private sealed class MockTransactionStore : ITransactionStore
        {
            public Func<TransactionId, bool, CancellationToken, Task<Transaction>> GetAsyncFunc { get; set; } = (_, _, _) => Task.FromResult<Transaction>(null!);

            public Task<Transaction> GetAsync(TransactionId id, bool forceFetch, CancellationToken cancellationToken)
            {
                return GetAsyncFunc(id, forceFetch, cancellationToken);
            }

            public Task<ImmutableList<Transaction>> ListAsync(int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Dictionary<string, AttributeValue> GetKey(TransactionId id) => throw new NotImplementedException();
            public Task<bool> ContainsAsync(TransactionId id, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task AddAsync(Transaction transaction, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<Transaction> UpdateAsync(Transaction transaction, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<Transaction> AppendRequestAsync(Transaction transaction, AmazonDynamoDBRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task RemoveAsync(TransactionId id, CancellationToken cancellationToken) => throw new NotImplementedException();
        }

        private sealed class MockAmazonDynamoDBKeyService : IAmazonDynamoDBKeyService
        {
            public Func<string, ImmutableDictionary<string, AttributeValue>, CancellationToken, Task<ImmutableDictionary<string, AttributeValue>>> CreateKeyMapAsyncFunc { get; set; } = (_, map, _) => Task.FromResult(map);

            public Task<ImmutableDictionary<string, AttributeValue>> CreateKeyMapAsync(string tableName, ImmutableDictionary<string, AttributeValue> item, CancellationToken cancellationToken)
            {
                return CreateKeyMapAsyncFunc(tableName, item, cancellationToken);
            }
        }

        private sealed class MockOptionsSnapshot<T> : IOptionsSnapshot<T> where T : class, new()
        {
            private readonly T _value;
            public MockOptionsSnapshot(T value) => _value = value;
            public T Value => _value;
            public T Get(string? name) => _value;
        }

        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            public Func<TransactWriteItemsRequest, CancellationToken, Task<TransactWriteItemsResponse>> TransactWriteItemsAsyncFunc { get; set; } = (_, _) => Task.FromResult(new TransactWriteItemsResponse());
            public Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>> TransactGetItemsAsyncFunc { get; set; } = (_, _) => Task.FromResult(new TransactGetItemsResponse { Responses = [] });
            public Func<GetItemRequest, CancellationToken, Task<GetItemResponse>> GetItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new GetItemResponse { Item = [] });
            public Func<PutItemRequest, CancellationToken, Task<PutItemResponse>> PutItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new PutItemResponse { Attributes = [] });
            public Func<UpdateItemRequest, CancellationToken, Task<UpdateItemResponse>> UpdateItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new UpdateItemResponse { Attributes = [] });

            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default)
            {
                return TransactWriteItemsAsyncFunc(request, cancellationToken);
            }

            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default)
            {
                return TransactGetItemsAsyncFunc(request, cancellationToken);
            }

            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default)
            {
                return GetItemAsyncFunc(request, cancellationToken);
            }

            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
            {
                return PutItemAsyncFunc(request, cancellationToken);
            }

            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default)
            {
                return UpdateItemAsyncFunc(request, cancellationToken);
            }

            // Implement required interface members with NotImplementedException for unused methods
            public IClientConfig Config => throw new NotImplementedException();
            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();
            public IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
            public void Dispose() { }
        }

        private static VersionedItemStore CreateStore(
            MockRequestService? requestService = null,
            MockTransactionServiceEvents? transactionServiceEvents = null,
            MockTransactionStore? transactionStore = null,
            MockAmazonDynamoDBKeyService? amazonDynamoDBKeyService = null,
            MockAmazonDynamoDB? amazonDynamoDB = null,
            AmazonDynamoDBOptions? options = null)
        {
            var optionsSnapshot = new MockOptionsSnapshot<AmazonDynamoDBOptions>(options ?? new AmazonDynamoDBOptions
            {
                TransactGetItemCountMaxValue = 100,
                TransactWriteItemCountMaxValue = 100
            });
            return new VersionedItemStore(
                optionsSnapshot,
                requestService ?? new MockRequestService(),
                transactionServiceEvents ?? new MockTransactionServiceEvents(),
                transactionStore ?? new MockTransactionStore(),
                amazonDynamoDBKeyService ?? new MockAmazonDynamoDBKeyService(),
                amazonDynamoDB ?? new MockAmazonDynamoDB());
        }

        private static Transaction CreateTransaction(string id)
        {
            return new Transaction(id, TransactionState.Active, 1, DateTime.UtcNow, ImmutableList<RequestRecord>.Empty);
        }

        private static ItemKey CreateItemKey(string tableName, string keyName, string keyValue)
        {
            var keyDict = ImmutableDictionary<string, AttributeValue>.Empty
                .Add(keyName, AttributeValueFactory.CreateS(keyValue));
            return ItemKey.Create(tableName, keyDict);
        }

        private static LockedItemRequestAction CreateLockedItemRequestAction(ItemKey itemKey, int requestId, RequestAction action)
        {
            return new LockedItemRequestAction(itemKey, requestId, action);
        }

        private static ItemRequestDetail CreateItemRequestDetail(ItemKey itemKey, RequestAction action, string? conditionExpression = null)
        {
            return new ItemRequestDetail(
                itemKey,
                action,
                conditionExpression,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
        }

        // ============================================================================
        // AcquireLocksAsync Tests
        // ============================================================================

        [TestMethod]
        public async Task AcquireLocksAsyncCallsOnAcquireLockAsync()
        {
            var onAcquireLockCalled = false;
            var transactionServiceEvents = new MockTransactionServiceEvents
            {
                OnAcquireLockAsync = (_, _, _) =>
                {
                    onAcquireLockCalled = true;
                    return Task.CompletedTask;
                }
            };

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty),
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList<ItemRequestDetail>.Empty)
            };

            var store = CreateStore(requestService: requestService, transactionServiceEvents: transactionServiceEvents);
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue>() };

            await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.IsTrue(onAcquireLockCalled);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncReturnsEmptyWhenNoItems()
        {
            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty),
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList<ItemRequestDetail>.Empty)
            };

            var store = CreateStore(requestService: requestService);
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue>() };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncSuccessfullyAcquiresLockForSingleItem()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Get);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Get);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction)),
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var store = CreateStore(requestService: requestService);
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.IsTrue(result.ContainsKey(itemKey));
            Assert.AreEqual("test-tx", result[itemKey].TransactionId);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncThrowsTransactionConflictedExceptionOnConflict()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Get);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Get);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction)),
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var conflictingItem = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("other-tx") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    var exception = new TransactionCanceledException("Transaction cancelled")
                    {
                        CancellationReasons = new List<CancellationReason>
                        {
                            new CancellationReason { Code = "ConditionalCheckFailed", Item = conflictingItem }
                        }
                    };
                    throw exception;
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };

            await Assert.ThrowsExceptionAsync<TransactionConflictedException>(async () =>
            {
                await store.AcquireLocksAsync(transaction, request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task AcquireLocksAsyncRetriesOnNonConflictFailure()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Put);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction)),
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var attemptCount = 0;
            var itemWithData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    attemptCount++;
                    if (attemptCount == 1)
                    {
                        var exception = new TransactionCanceledException("Transaction cancelled")
                        {
                            CancellationReasons = new List<CancellationReason>
                            {
                                new CancellationReason { Code = "ConditionalCheckFailed", Item = itemWithData }
                            }
                        };
                        throw exception;
                    }
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } }
            };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(2, attemptCount);
            Assert.AreEqual(1, result.Count);
        }

        // ============================================================================
        // GetItemRecordAndTransactionState Tests
        // ============================================================================

        [TestMethod]
        public void GetItemRecordAndTransactionStateReturnsCorrectTransientState()
        {
            var store = CreateStore();
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");

            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsTrue(result.TransactionStateValue.IsTransient);
            Assert.AreEqual("tx-123", result.TransactionStateValue.TransactionId);
            Assert.AreEqual(0, result.ItemResponse.AttributeValues.Count); // Transient and not applied means empty
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateReturnsDataWhenApplied()
        {
            var store = CreateStore();
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");

            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsTrue(result.TransactionStateValue.IsTransient);
            Assert.IsTrue(result.TransactionStateValue.IsApplied);
            Assert.AreEqual(2, result.ItemResponse.AttributeValues.Count); // Id and Data
            Assert.IsTrue(result.ItemResponse.AttributeValues.ContainsKey("Id"));
            Assert.IsTrue(result.ItemResponse.AttributeValues.ContainsKey("Data"));
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithoutItemKeyReturnsCorrectState()
        {
            var store = CreateStore();

            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            var result = store.GetItemRecordAndTransactionState(item);

            Assert.AreEqual("tx-123", result.Item2.TransactionId);
            Assert.IsFalse(result.Item2.IsTransient);
            Assert.IsFalse(result.Item2.IsApplied);
            Assert.AreEqual(2, result.Item1.Count);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateFiltersOutTransactionAttributes()
        {
            var store = CreateStore();
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");

            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsFalse(result.ItemResponse.AttributeValues.ContainsKey(ItemAttributeName.TXID.Value));
            Assert.IsFalse(result.ItemResponse.AttributeValues.ContainsKey(ItemAttributeName.APPLIED.Value));
            Assert.IsFalse(result.ItemResponse.AttributeValues.ContainsKey(ItemAttributeName.DATE.Value));
            Assert.IsTrue(result.ItemResponse.AttributeValues.ContainsKey("Id"));
            Assert.IsTrue(result.ItemResponse.AttributeValues.ContainsKey("Data"));
        }

        // ============================================================================
        // GetItemsToBackupAsync Tests
        // ============================================================================

        [TestMethod]
        public async Task GetItemsToBackupAsyncReturnsEmptyForNoMutatingRequests()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Get);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var store = CreateStore(requestService: requestService);
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncReturnsItemsForMutatingRequests()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Put);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable", Item = itemData };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(itemKey, result[0].Key);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncExcludesAppliedItems()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Update);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } }
            };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncExcludesTransientItems()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Delete);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("true") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } }
            };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        // ============================================================================
        // ReleaseLocksAsync Tests
        // ============================================================================

        [TestMethod]
        public async Task ReleaseLocksAsyncCallsOnReleaseLocksAsync()
        {
            var onReleaseLocksCalled = false;
            var transactionServiceEvents = new MockTransactionServiceEvents
            {
                OnReleaseLocksAsync = (_, _, _) =>
                {
                    onReleaseLocksCalled = true;
                    return Task.CompletedTask;
                }
            };

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };

            var store = CreateStore(requestService: requestService, transactionServiceEvents: transactionServiceEvents);
            var transaction = CreateTransaction("test-tx");

            await store.ReleaseLocksAsync(transaction, false, ImmutableDictionary<ItemKey, ItemRecord>.Empty, CancellationToken.None);

            Assert.IsTrue(onReleaseLocksCalled);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncDoesNothingWhenNoItems()
        {
            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };

            var transactWriteCalled = false;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) => Task.FromResult(new TransactGetItemsResponse { Responses = [] }),
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    transactWriteCalled = true;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");

            await store.ReleaseLocksAsync(transaction, false, ImmutableDictionary<ItemKey, ItemRecord>.Empty, CancellationToken.None);

            Assert.IsFalse(transactWriteCalled);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncCommitsDeleteOperation()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("test-tx") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                },
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");

            await store.ReleaseLocksAsync(transaction, false, ImmutableDictionary<ItemKey, ItemRecord>.Empty, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Delete);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncRollsBackWithImage()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("test-tx") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            var rollbackImage = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-id")))
                    .Add("OldData", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("old-data"))));

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                },
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, rollbackImage);

            await store.ReleaseLocksAsync(transaction, true, rollbackImages, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Put);
            Assert.IsTrue(capturedRequest.TransactItems[0].Put.Item.ContainsKey("OldData"));
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncDeletesTransientItemOnRollback()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Get);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("test-tx") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                },
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");

            await store.ReleaseLocksAsync(transaction, true, ImmutableDictionary<ItemKey, ItemRecord>.Empty, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Delete);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncRemovesTransactionAttributesOnCommit()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put);

            var requestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(lockedAction))
            };

            var itemData = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("test-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("test-tx") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) }
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = itemData }
                        }
                    });
                },
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");

            await store.ReleaseLocksAsync(transaction, false, ImmutableDictionary<ItemKey, ItemRecord>.Empty, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Update);
            Assert.IsTrue(capturedRequest.TransactItems[0].Update.UpdateExpression.Contains("REMOVE"));
        }

        // ============================================================================
        // ReleaseLocksAsync (Other Transaction) Tests
        // ============================================================================

        [TestMethod]
        public async Task ReleaseLocksAsyncWithOtherTransactionCallsOnReleaseLockFromOtherTransactionAsync()
        {
            var onReleaseLockFromOtherTransactionCalled = false;
            var transactionServiceEvents = new MockTransactionServiceEvents
            {
                OnReleaseLockFromOtherTransactionAsync = (_, _, _) =>
                {
                    onReleaseLockFromOtherTransactionCalled = true;
                    return Task.CompletedTask;
                }
            };

            var store = CreateStore(transactionServiceEvents: transactionServiceEvents);
            var id = new TransactionId("test-tx-1");
            var owningTransactionId = new TransactionId("test-tx-2");

            await store.ReleaseLocksAsync(
                id,
                owningTransactionId,
                ImmutableList<ItemKey>.Empty,
                true,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                CancellationToken.None);

            Assert.IsTrue(onReleaseLockFromOtherTransactionCalled);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithOtherTransactionThrowsWhenRollbackIsFalse()
        {
            var store = CreateStore();
            var id = new TransactionId("test-tx-1");
            var owningTransactionId = new TransactionId("test-tx-2");

            await Assert.ThrowsExceptionAsync<ArgumentException>(async () =>
            {
                await store.ReleaseLocksAsync(
                    id,
                    owningTransactionId,
                    ImmutableList<ItemKey>.Empty,
                    false,
                    ImmutableDictionary<ItemKey, ItemTransactionState>.Empty,
                    ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                    CancellationToken.None);
            });
        }

        // ============================================================================
        // ApplyRequestAsync Tests - GetItemRequest
        // ============================================================================

        [TestMethod]
        public async Task ApplyRequestAsyncCallsOnApplyRequestAsync()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var onApplyRequestCalled = false;
            var transactionServiceEvents = new MockTransactionServiceEvents
            {
                OnApplyRequestAsync = (_, _, _) =>
                {
                    onApplyRequestCalled = true;
                    return Task.CompletedTask;
                }
            };

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "test-tx",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 0, RequestAction.Get));

            var store = CreateStore(transactionServiceEvents: transactionServiceEvents);
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsTrue(onApplyRequestCalled);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithGetItemRequestReturnsEmptyForDeletedItem()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (GetItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithGetItemRequestReturnsEmptyForTransientGetItem()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "tx-123",
                DateTime.UtcNow,
                true,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Get));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (GetItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithGetItemRequestReturnsDataFromBackup()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put));
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-id")))
                    .Add("Data", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-data"))));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = (GetItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(2, result.Item.Count);
            Assert.IsTrue(result.Item.ContainsKey("Id"));
            Assert.IsTrue(result.Item.ContainsKey("Data"));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithGetItemRequestFetchesFromDatabase()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Get));

            var dbItem = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("db-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse { Item = dbItem })
            };

            var store = CreateStore(amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } } };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (GetItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(2, result.Item.Count);
            Assert.IsTrue(result.Item.ContainsKey("Data"));
            Assert.IsFalse(result.Item.ContainsKey(ItemAttributeName.TXID.Value));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithGetItemRequestRespectsProjectionExpression()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put));
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-id")))
                    .Add("Data", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-data")))
                    .Add("Extra", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("extra-data"))));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ProjectionExpression = "Id, Data",
                ExpressionAttributeNames = new Dictionary<string, string>()
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = (GetItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(2, result.Item.Count);
            Assert.IsTrue(result.Item.ContainsKey("Id"));
            Assert.IsTrue(result.Item.ContainsKey("Data"));
            Assert.IsFalse(result.Item.ContainsKey("Extra"));
        }

        // ============================================================================
        // ApplyRequestAsync Tests - DeleteItemRequest
        // ============================================================================

        [TestMethod]
        public async Task ApplyRequestAsyncWithDeleteItemRequestReturnsCorrectResponse()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete));
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-id")))
                    .Add("Data", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-data"))));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.ALL_OLD
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = (DeleteItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(2, result.Attributes.Count);
            Assert.IsTrue(result.Attributes.ContainsKey("Id"));
            Assert.IsTrue(result.Attributes.ContainsKey("Data"));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithDeleteItemRequestReturnsEmptyForTransientItem()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "tx-123",
                DateTime.UtcNow,
                true,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.ALL_OLD
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (DeleteItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Attributes.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithDeleteItemRequestReturnsEmptyForNoneReturnValue()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete));

            var store = CreateStore();
            var transaction = CreateTransaction("test-tx");
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.NONE
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (DeleteItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Attributes.Count);
        }

        // ============================================================================
        // ApplyRequestAsync Tests - PutItemRequest
        // ============================================================================

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemRequestAppliesWhenNotApplied()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "tx-123",
                DateTime.UtcNow,
                true,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put));

            var putResponse = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("new-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) =>
                {
                    Assert.IsTrue(req.Item.ContainsKey(ItemAttributeName.TXID.Value));
                    Assert.IsTrue(req.Item.ContainsKey(ItemAttributeName.APPLIED.Value));
                    Assert.IsTrue(req.Item.ContainsKey(ItemAttributeName.TRANSIENT.Value));
                    return Task.FromResult(new PutItemResponse { Attributes = putResponse });
                }
            };

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(CreateItemRequestDetail(itemKey, RequestAction.Put)))
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("test-id") },
                    { "Data", AttributeValueFactory.CreateS("new-data") }
                },
                ReturnValues = ReturnValue.ALL_NEW,
                ExpressionAttributeNames = new Dictionary<string, string>(),
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (PutItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Attributes.Count >= 2);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemRequestReturnsEmptyWhenAlreadyAppliedAndReturnNone()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                true,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put));

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(CreateItemRequestDetail(itemKey, RequestAction.Put)))
            };

            var store = CreateStore(requestService: requestService);
            var transaction = CreateTransaction("tx-123");
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.NONE
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (PutItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Attributes.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemRequestReturnsAllOldForTransient()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "tx-123",
                DateTime.UtcNow,
                true,
                true,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put));

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(CreateItemRequestDetail(itemKey, RequestAction.Put)))
            };

            var store = CreateStore(requestService: requestService);
            var transaction = CreateTransaction("tx-123");
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.ALL_OLD
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (PutItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Attributes.Count);
        }

        // ============================================================================
        // ApplyRequestAsync Tests - UpdateItemRequest
        // ============================================================================

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemRequestAppliesWhenNotApplied()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Update));

            var updateResponse = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id") },
                { "Data", AttributeValueFactory.CreateS("updated-data") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("true") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) =>
                {
                    Assert.IsTrue(req.ConditionExpression.Contains(ItemAttributeName.TXID.Value));
                    Assert.IsTrue(req.ConditionExpression.Contains(ItemAttributeName.APPLIED.Value));
                    Assert.IsTrue(req.UpdateExpression.Contains(ItemAttributeName.APPLIED.Value));
                    return Task.FromResult(new UpdateItemResponse { Attributes = updateResponse });
                }
            };

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(CreateItemRequestDetail(itemKey, RequestAction.Update)))
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                UpdateExpression = "SET #data = :data",
                ReturnValues = ReturnValue.ALL_NEW,
                ExpressionAttributeNames = new Dictionary<string, string> { { "#data", "Data" } },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":data", AttributeValueFactory.CreateS("updated-data") } }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (UpdateItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemRequestReturnsEmptyWhenAlreadyApplied()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                true,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Update));

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(CreateItemRequestDetail(itemKey, RequestAction.Update)))
            };

            var store = CreateStore(requestService: requestService);
            var transaction = CreateTransaction("tx-123");
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.NONE
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (UpdateItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(0, result.Attributes.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemRequestReturnsAllOldFromBackup()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                true,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Update));
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test-id")))
                    .Add("Data", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("old-data"))));

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(CreateItemRequestDetail(itemKey, RequestAction.Update)))
            };

            var store = CreateStore(requestService: requestService);
            var transaction = CreateTransaction("tx-123");
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                ReturnValues = ReturnValue.ALL_NEW
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = (UpdateItemResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(2, result.Attributes.Count);
            Assert.IsTrue(result.Attributes.ContainsKey("Data"));
        }

        // ============================================================================
        // ApplyRequestAsync Tests - TransactGetItemsRequest
        // ============================================================================

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactGetItemsRequestFetchesMultipleItems()
        {
            var itemKey1 = CreateItemKey("TestTable", "Id", "test-id-1");
            var itemKey2 = CreateItemKey("TestTable", "Id", "test-id-2");

            var itemTransactionState1 = new ItemTransactionState(
                itemKey1,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey1, 1, RequestAction.Get));

            var itemTransactionState2 = new ItemTransactionState(
                itemKey2,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey2, 2, RequestAction.Get));

            var dbItem1 = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id-1") },
                { "Data", AttributeValueFactory.CreateS("data-1") }
            };

            var dbItem2 = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("test-id-2") },
                { "Data", AttributeValueFactory.CreateS("data-2") }
            };

            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = dbItem1 },
                            new ItemResponse { Item = dbItem2 }
                        }
                    });
                }
            };

            var store = CreateStore(amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id-1") } }
                        }
                    },
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id-2") } }
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty
                    .Add(itemKey1, itemTransactionState1)
                    .Add(itemKey2, itemTransactionState2),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (TransactGetItemsResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(2, result.Responses.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactGetItemsRequestSkipsDeletedItems()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete));

            var store = CreateStore();
            var transaction = CreateTransaction("tx-123");
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } }
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = (TransactGetItemsResponse)await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.AreEqual(1, result.Responses.Count);
            Assert.AreEqual(0, result.Responses[0].Item.Count);
        }

        // ============================================================================
        // ApplyRequestAsync Tests - TransactWriteItemsRequest
        // ============================================================================

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactWriteItemsRequestProcessesPutOperation()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Put);

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "tx-123",
                DateTime.UtcNow,
                true,
                false,
                lockedAction);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) =>
                {
                    return Task.FromResult(item.Where(kv => kv.Key == "Id").ToImmutableDictionary());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB, amazonDynamoDBKeyService: amazonDynamoDBKeyService);
            var transaction = CreateTransaction("tx-123");
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
                                { "Data", AttributeValueFactory.CreateS("new-data") }
                            },
                            ExpressionAttributeNames = new Dictionary<string, string>(),
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Put);
            Assert.IsTrue(capturedRequest.TransactItems[0].Put.Item.ContainsKey(ItemAttributeName.TXID.Value));
            Assert.IsTrue(capturedRequest.TransactItems[0].Put.Item.ContainsKey(ItemAttributeName.APPLIED.Value));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactWriteItemsRequestProcessesUpdateOperation()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Update);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Update);

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                lockedAction);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                            UpdateExpression = "SET #data = :data",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#data", "Data" } },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":data", AttributeValueFactory.CreateS("updated-data") } }
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Update);
            Assert.IsTrue(capturedRequest.TransactItems[0].Update.UpdateExpression.Contains(ItemAttributeName.APPLIED.Value));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactWriteItemsRequestSkipsDeleteOperation()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Delete);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Delete);

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                lockedAction);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } }
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(result);
            // When all items are delete operations and filtered out, the request is not sent to DynamoDB
            // and the method returns early with an empty response
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactWriteItemsRequestConvertsAlreadyAppliedToConditionCheck()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.Put);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.Put);

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                true, // Already applied
                lockedAction);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService
            {
                CreateKeyMapAsyncFunc = (tableName, item, ct) =>
                {
                    return Task.FromResult(item.Where(kv => kv.Key == "Id").ToImmutableDictionary());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB, amazonDynamoDBKeyService: amazonDynamoDBKeyService);
            var transaction = CreateTransaction("tx-123");
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                            ExpressionAttributeNames = new Dictionary<string, string>(),
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].ConditionCheck);
            Assert.IsTrue(capturedRequest.TransactItems[0].ConditionCheck.ConditionExpression.Contains(ItemAttributeName.APPLIED.Value));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactWriteItemsRequestProcessesConditionCheckAttributeNotExists()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.ConditionCheck);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.ConditionCheck, "attribute_not_exists(Id)");

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                "tx-123",
                DateTime.UtcNow,
                true,
                false,
                lockedAction);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                            ConditionExpression = "attribute_not_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>(),
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].ConditionCheck);
            Assert.IsTrue(capturedRequest.TransactItems[0].ConditionCheck.ConditionExpression.Contains(ItemAttributeName.TRANSIENT.Value));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactWriteItemsRequestProcessesConditionCheckAttributeExists()
        {
            var itemKey = CreateItemKey("TestTable", "Id", "test-id");
            var lockedAction = CreateLockedItemRequestAction(itemKey, 1, RequestAction.ConditionCheck);
            var itemDetail = CreateItemRequestDetail(itemKey, RequestAction.ConditionCheck, "attribute_exists(Id)");

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                "tx-123",
                DateTime.UtcNow,
                false,
                false,
                lockedAction);

            var requestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (_, _) => Task.FromResult(ImmutableList.Create(itemDetail))
            };

            TransactWriteItemsRequest? capturedRequest = null;
            var amazonDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var store = CreateStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var transaction = CreateTransaction("tx-123");
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", AttributeValueFactory.CreateS("test-id") } },
                            ConditionExpression = "attribute_exists(Id)",
                            ExpressionAttributeNames = new Dictionary<string, string>(),
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].ConditionCheck);
            Assert.IsTrue(capturedRequest.TransactItems[0].ConditionCheck.ConditionExpression.Contains("attribute_not_exists"));
            Assert.IsTrue(capturedRequest.TransactItems[0].ConditionCheck.ConditionExpression.Contains(ItemAttributeName.TRANSIENT.Value));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUnsupportedRequestTypeThrowsNotSupportedException()
        {
            var store = CreateStore();
            var transaction = CreateTransaction("tx-123");
            var request = new ScanRequest { TableName = "TestTable" };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await store.ApplyRequestAsync(applyRequest, CancellationToken.None);
            });
        }
    }
}
