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
using System.Globalization;
using System.IO;
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

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class VersionedItemStoreTests
    {
        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            private readonly Func<TransactWriteItemsRequest, CancellationToken, Task<TransactWriteItemsResponse>>? _transactWriteItemsAsync;
            private readonly Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>>? _transactGetItemsAsync;
            private readonly Func<UpdateItemRequest, CancellationToken, Task<UpdateItemResponse>>? _updateItemAsync;
            private readonly Func<PutItemRequest, CancellationToken, Task<PutItemResponse>>? _putItemAsync;
            private readonly Func<GetItemRequest, CancellationToken, Task<GetItemResponse>>? _getItemAsync;

            public MockAmazonDynamoDB(
                Func<TransactWriteItemsRequest, CancellationToken, Task<TransactWriteItemsResponse>>? transactWriteItemsAsync = null,
                Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>>? transactGetItemsAsync = null,
                Func<UpdateItemRequest, CancellationToken, Task<UpdateItemResponse>>? updateItemAsync = null,
                Func<PutItemRequest, CancellationToken, Task<PutItemResponse>>? putItemAsync = null,
                Func<GetItemRequest, CancellationToken, Task<GetItemResponse>>? getItemAsync = null)
            {
                _transactWriteItemsAsync = transactWriteItemsAsync;
                _transactGetItemsAsync = transactGetItemsAsync;
                _updateItemAsync = updateItemAsync;
                _putItemAsync = putItemAsync;
                _getItemAsync = getItemAsync;
            }

            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default)
            {
                return _transactWriteItemsAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new TransactWriteItemsResponse());
            }

            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default)
            {
                return _transactGetItemsAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new TransactGetItemsResponse());
            }

            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default)
            {
                return _updateItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new UpdateItemResponse());
            }

            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
            {
                return _putItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new PutItemResponse());
            }

            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default)
            {
                return _getItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new GetItemResponse());
            }

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
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();
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
            public IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
            public void Dispose() { }
        }

        private sealed class MockRequestService : IRequestService
        {
            private readonly Func<Transaction, CancellationToken, Task<ImmutableList<LockedItemRequestAction>>>? _getItemRequestActionsAsync;
            private readonly Func<AmazonDynamoDBRequest, CancellationToken, Task<ImmutableList<ItemRequestDetail>>>? _getItemRequestDetailsAsync;

            public MockRequestService(
                Func<Transaction, CancellationToken, Task<ImmutableList<LockedItemRequestAction>>>? getItemRequestActionsAsync = null,
                Func<AmazonDynamoDBRequest, CancellationToken, Task<ImmutableList<ItemRequestDetail>>>? getItemRequestDetailsAsync = null)
            {
                _getItemRequestActionsAsync = getItemRequestActionsAsync;
                _getItemRequestDetailsAsync = getItemRequestDetailsAsync;
            }

            public Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(Transaction transaction, CancellationToken cancellationToken)
            {
                return _getItemRequestActionsAsync?.Invoke(transaction, cancellationToken)
                    ?? Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty);
            }

            public Task<ImmutableList<ItemRequestDetail>> GetItemRequestDetailsAsync(AmazonDynamoDBRequest request, CancellationToken cancellationToken)
            {
                return _getItemRequestDetailsAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(ImmutableList<ItemRequestDetail>.Empty);
            }
        }

        private sealed class MockTransactionServiceEvents : ITransactionServiceEvents
        {
            public Func<TransactionId, CancellationToken, Task>? OnResumeTransactionFinishAsync { get; set; }
            public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnApplyRequestAsync { get; set; }
            public Func<TransactionVersion, CancellationToken, Task>? OnUpdateFullyAppliedRequestsBeginAsync { get; set; }
            public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnAcquireLockAsync { get; set; }
            public Func<TransactionId, bool, CancellationToken, Task>? OnReleaseLocksAsync { get; set; }
            public Func<TransactionId, TransactionId, CancellationToken, Task>? OnReleaseLockFromOtherTransactionAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnBackupItemImagesAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task<bool>>? OnDoCommitBeginAsync { get; set; }
            public Func<TransactionId, CancellationToken, Task>? OnDoRollbackBeginAsync { get; set; }
        }

        private sealed class MockTransactionStore : ITransactionStore
        {
            private readonly Func<TransactionId, bool, CancellationToken, Task<Transaction>>? _getAsync;

            public MockTransactionStore(
                Func<TransactionId, bool, CancellationToken, Task<Transaction>>? getAsync = null)
            {
                _getAsync = getAsync;
            }

            public Task<Transaction> GetAsync(TransactionId id, bool forceFetch, CancellationToken cancellationToken)
            {
                return _getAsync?.Invoke(id, forceFetch, cancellationToken)
                    ?? Task.FromResult(CreateTransaction());
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
            private readonly Func<string, ImmutableDictionary<string, AttributeValue>, CancellationToken, Task<ImmutableDictionary<string, AttributeValue>>>? _createKeyMapAsync;

            public MockAmazonDynamoDBKeyService(
                Func<string, ImmutableDictionary<string, AttributeValue>, CancellationToken, Task<ImmutableDictionary<string, AttributeValue>>>? createKeyMapAsync = null)
            {
                _createKeyMapAsync = createKeyMapAsync;
            }

            public Task<ImmutableDictionary<string, AttributeValue>> CreateKeyMapAsync(string tableName, ImmutableDictionary<string, AttributeValue> item, CancellationToken cancellationToken)
            {
                return _createKeyMapAsync?.Invoke(tableName, item, cancellationToken)
                    ?? Task.FromResult(ImmutableDictionary<string, AttributeValue>.Empty);
            }
        }

        private static VersionedItemStore CreateVersionedItemStore(
            AmazonDynamoDBOptions? options = null,
            IRequestService? requestService = null,
            ITransactionServiceEvents? transactionServiceEvents = null,
            ITransactionStore? transactionStore = null,
            IAmazonDynamoDBKeyService? amazonDynamoDBKeyService = null,
            IAmazonDynamoDB? amazonDynamoDB = null)
        {
            var optionsSnapshot = new MockOptionsSnapshot<AmazonDynamoDBOptions>(options ?? new AmazonDynamoDBOptions());
            return new VersionedItemStore(
                optionsSnapshot,
                requestService ?? new MockRequestService(),
                transactionServiceEvents ?? new MockTransactionServiceEvents(),
                transactionStore ?? new MockTransactionStore(),
                amazonDynamoDBKeyService ?? new MockAmazonDynamoDBKeyService(),
                amazonDynamoDB ?? new MockAmazonDynamoDB());
        }

        private sealed class MockOptionsSnapshot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T> : IOptionsSnapshot<T> where T : class
        {
            private readonly T _value;

            public MockOptionsSnapshot(T value)
            {
                _value = value;
            }

            public T Value => _value;
            public T Get(string? name) => _value;
        }

        private static ItemKey CreateItemKey(string tableName = "TestTable", string keyName = "Id", string keyValue = "TestId")
        {
            return ItemKey.Create(tableName, new Dictionary<string, AttributeValue>
            {
                { keyName, AttributeValueFactory.CreateS(keyValue) }
            }.ToImmutableDictionary());
        }

        private static Transaction CreateTransaction(string id = "tx-123", TransactionState state = TransactionState.Active)
        {
            return new Transaction(
                id,
                state,
                1,
                DateTime.UtcNow,
                ImmutableList<RequestRecord>.Empty);
        }

        private static ItemTransactionState CreateItemTransactionState(ItemKey itemKey, Transaction transaction, RequestAction action = RequestAction.Put, bool isApplied = true)
        {
            return new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                isApplied,
                new LockedItemRequestAction(itemKey, 0, action));
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithItemKeyReturnsCorrectValues()
        {
            var store = CreateVersionedItemStore();
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN("638000000000000000") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("1") }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.AreEqual(itemKey, result.ItemResponse.Key);
            Assert.AreEqual(2, result.ItemResponse.AttributeValues.Count);
            Assert.IsTrue(result.ItemResponse.AttributeValues.ContainsKey("Id"));
            Assert.IsTrue(result.ItemResponse.AttributeValues.ContainsKey("Name"));
            Assert.AreEqual("tx-123", result.TransactionStateValue.TransactionId);
            Assert.IsTrue(result.TransactionStateValue.IsTransient);
            Assert.IsTrue(result.TransactionStateValue.IsApplied);
            Assert.IsTrue(result.TransactionStateValue.Exists);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithItemKeyReturnsEmptyForTransientNotApplied()
        {
            var store = CreateVersionedItemStore();
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.ItemResponse.AttributeValues.Count);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithoutItemKeyReturnsCorrectValues()
        {
            var store = CreateVersionedItemStore();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") }
            };

            var result = store.GetItemRecordAndTransactionState(item);

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Item1.Count);
            Assert.IsTrue(result.Item1.ContainsKey("Name"));
            Assert.AreEqual("tx-123", result.Item2.TransactionId);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithoutItemKeyReturnsEmptyForTransientNotApplied()
        {
            var store = CreateVersionedItemStore();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") }
            };

            var result = store.GetItemRecordAndTransactionState(item);

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Item1.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncReturnsEmptyWhenNoMutatingRequests()
        {
            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        CreateItemKey(),
                        RequestAction.Get,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));
            var store = CreateVersionedItemStore(requestService: requestService);
            var request = new GetItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncReturnsItemsForMutatingRequests()
        {
            var itemKey = CreateItemKey();
            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { "Value", AttributeValueFactory.CreateS("TestValue") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(itemKey, result[0].Key);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncSucceedsOnFirstAttempt()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.IsTrue(result.ContainsKey(itemKey));
        }

        [TestMethod]
        public async Task AcquireLocksAsyncCallsEventHook()
        {
            var called = false;
            var events = new MockTransactionServiceEvents
            {
                OnAcquireLockAsync = (_, _, _) =>
                {
                    called = true;
                    return Task.CompletedTask;
                }
            };

            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                transactionServiceEvents: events,
                amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemRequestSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var putItemRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") },
                    { "Value", AttributeValueFactory.CreateS("NewValue") }
                }
            };

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                putItemAsync: (req, _) =>
                {
                    Assert.AreEqual("TestTable", req.TableName);
                    return Task.FromResult(new PutItemResponse());
                });

            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var applyRequest = new ApplyRequestRequest(
                transaction,
                putItemRequest,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(PutItemResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemRequestSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var updateItemRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                UpdateExpression = "SET #v = :val",
                ExpressionAttributeNames = new Dictionary<string, string> { { "#v", "Value" } },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":val", AttributeValueFactory.CreateS("UpdatedValue") }
                }
            };

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                updateItemAsync: (req, _) =>
                {
                    Assert.AreEqual("TestTable", req.TableName);
                    return Task.FromResult(new UpdateItemResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var applyRequest = new ApplyRequestRequest(
                transaction,
                updateItemRequest,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, CreateItemTransactionState(itemKey, transaction, RequestAction.Update)),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(UpdateItemResponse));
        }


        [TestMethod]
        public async Task ApplyRequestAsyncCallsEventHook()
        {
            var called = false;
            var events = new MockTransactionServiceEvents
            {
                OnApplyRequestAsync = (_, _, _) =>
                {
                    called = true;
                    return Task.CompletedTask;
                }
            };

            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var putItemRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                }
            };

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var store = CreateVersionedItemStore(requestService: requestService, transactionServiceEvents: events);
            var applyRequest = new ApplyRequestRequest(
                transaction,
                putItemRequest,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, CreateItemTransactionState(itemKey, transaction)),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, _) =>
                {
                    Assert.IsNotNull(req.TransactItems);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, false, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncCallsEventHook()
        {
            var called = false;
            var events = new MockTransactionServiceEvents
            {
                OnReleaseLocksAsync = (_, _, _) =>
                {
                    called = true;
                    return Task.CompletedTask;
                }
            };

            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                transactionServiceEvents: events,
                amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, false, rollbackImages, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithEmptyItemsDoesNothing()
        {
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty));

            var transactWriteCalled = false;
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) =>
                {
                    transactWriteCalled = true;
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, false, rollbackImages, CancellationToken.None);

            Assert.IsFalse(transactWriteCalled);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithConflictThrowsTransactionConflictedException()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction("tx-123");
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) =>
                {
                    var ex = new TransactionCanceledException("Transaction cancelled");
                    ex.CancellationReasons = new List<CancellationReason>
                    {
                        new CancellationReason
                        {
                            Code = "ConditionalCheckFailed",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-456") }
                            }
                        }
                    };
                    throw ex;
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            await Assert.ThrowsExceptionAsync<TransactionConflictedException>(async () =>
            {
                await store.AcquireLocksAsync(transaction, request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task AcquireLocksAsyncRetrySucceedsOnSecondAttempt()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction("tx-123");
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var attemptCount = 0;
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) =>
                {
                    attemptCount++;
                    if (attemptCount == 1)
                    {
                        var ex = new TransactionCanceledException("Transaction cancelled");
                        ex.CancellationReasons = new List<CancellationReason>
                        {
                            new CancellationReason
                            {
                                Code = "ConditionalCheckFailed",
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") }
                                }
                            }
                        };
                        throw ex;
                    }
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(2, attemptCount);
            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncWithMultipleMutatingRequestsReturnsAllItems()
        {
            var itemKey1 = CreateItemKey("TestTable", "Id", "Id1");
            var itemKey2 = CreateItemKey("TestTable", "Id", "Id2");
            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey1,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new ItemRequestDetail(
                        itemKey2,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("Id1") },
                                { "Value", AttributeValueFactory.CreateS("Value1") }
                            }
                        },
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("Id2") },
                                { "Value", AttributeValueFactory.CreateS("Value2") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithoutTransactionIdReturnsNullTransactionId()
        {
            var store = CreateVersionedItemStore();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") }
            };

            var result = store.GetItemRecordAndTransactionState(item);

            Assert.IsNotNull(result);
            Assert.IsNull(result.Item2.TransactionId);
            Assert.IsTrue(result.Item2.Exists);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithItemKeyWithoutTransactionIdReturnsNullTransactionId()
        {
            var store = CreateVersionedItemStore();
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.IsNull(result.TransactionStateValue.TransactionId);
            Assert.IsTrue(result.TransactionStateValue.Exists);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateFiltersTransactionAttributes()
        {
            var store = CreateVersionedItemStore();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN("638000000000000000") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("0") },
                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("1") }
            };

            var result = store.GetItemRecordAndTransactionState(item);

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Item1.Count);
            Assert.IsTrue(result.Item1.ContainsKey("Id"));
            Assert.IsTrue(result.Item1.ContainsKey("Name"));
            Assert.IsFalse(result.Item1.ContainsKey(ItemAttributeName.TXID.Value));
            Assert.IsFalse(result.Item1.ContainsKey(ItemAttributeName.DATE.Value));
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithRollbackSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var rollbackImage = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty.Add(
                    "OldValue", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("OldValue"))));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, rollbackImage);

            await store.ReleaseLocksAsync(transaction, true, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemNotAppliedCallsDynamoDB()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var putItemRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") },
                    { "Value", AttributeValueFactory.CreateS("NewValue") }
                }
            };

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var putCalled = false;
            var amazonDynamoDB = new MockAmazonDynamoDB(
                putItemAsync: (req, _) =>
                {
                    putCalled = true;
                    return Task.FromResult(new PutItemResponse());
                });

            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Put, isApplied: false);

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var applyRequest = new ApplyRequestRequest(
                transaction,
                putItemRequest,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsTrue(putCalled);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemNotAppliedCallsDynamoDB()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var updateItemRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                UpdateExpression = "SET #v = :val",
                ExpressionAttributeNames = new Dictionary<string, string> { { "#v", "Value" } },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":val", AttributeValueFactory.CreateS("UpdatedValue") }
                }
            };

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var updateCalled = false;
            var amazonDynamoDB = new MockAmazonDynamoDB(
                updateItemAsync: (req, _) =>
                {
                    updateCalled = true;
                    return Task.FromResult(new UpdateItemResponse());
                });

            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Update, isApplied: false);

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var applyRequest = new ApplyRequestRequest(
                transaction,
                updateItemRequest,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsTrue(updateCalled);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithMultipleItemsSucceeds()
        {
            var itemKey1 = CreateItemKey("TestTable", "Id", "Id1");
            var itemKey2 = CreateItemKey("TestTable", "Id", "Id2");
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey1, 0, RequestAction.Put),
                    new LockedItemRequestAction(itemKey2, 1, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey1,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new ItemRequestDetail(
                        itemKey2,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(2, result.Count);
            Assert.IsTrue(result.ContainsKey(itemKey1));
            Assert.IsTrue(result.ContainsKey(itemKey2));
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncWithDeleteActionReturnsItems()
        {
            var itemKey = CreateItemKey();
            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Delete,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { "Value", AttributeValueFactory.CreateS("TestValue") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new DeleteItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncWithUpdateActionReturnsItems()
        {
            var itemKey = CreateItemKey();
            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { "Value", AttributeValueFactory.CreateS("TestValue") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new UpdateItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithAppliedFalseReturnsCorrectState()
        {
            var store = CreateVersionedItemStore();
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.TransactionStateValue.IsApplied);
            Assert.IsTrue(result.TransactionStateValue.IsTransient);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithNonExistentItemSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        "attribute_not_exists(#id)",
                        ImmutableDictionary<string, string>.Empty.Add("#id", "Id"),
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithMultipleItemsSucceeds()
        {
            var itemKey1 = CreateItemKey("TestTable", "Id", "Id1");
            var itemKey2 = CreateItemKey("TestTable", "Id", "Id2");
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey1, 0, RequestAction.Put),
                    new LockedItemRequestAction(itemKey2, 1, RequestAction.Put))));

            var transactWriteCount = 0;
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("Id1") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) }
                            }
                        },
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("Id2") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) }
                            }
                        }
                    }
                }),
                transactWriteItemsAsync: (req, _) =>
                {
                    transactWriteCount++;
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, false, rollbackImages, CancellationToken.None);

            Assert.IsTrue(transactWriteCount > 0);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactGetItemsAndDeleteActionReturnsEmpty()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Delete);
            var requestService = new MockRequestService();

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { "Value", AttributeValueFactory.CreateS("TestValue") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
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
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
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

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as TransactGetItemsResponse;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Responses.Count);
            Assert.AreEqual(0, result.Responses[0].Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithTransactGetItemsAndGetActionWithTransientReturnsEmpty()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                true,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Get));
            var requestService = new MockRequestService();

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
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
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
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

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as TransactGetItemsResponse;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Responses.Count);
            Assert.AreEqual(0, result.Responses[0].Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithProjectionExpressionFiltersAttributes()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestId")))
                    .Add("Name", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestName")))
                    .Add("Email", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test@example.com"))));
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Put);
            var requestService = new MockRequestService();

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { "Name", AttributeValueFactory.CreateS("TestName") },
                                { "Email", AttributeValueFactory.CreateS("test@example.com") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
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
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            },
                            ProjectionExpression = "#n, #e",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#n", "Name" },
                                { "#e", "Email" }
                            }
                        }
                    }
                }
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as TransactGetItemsResponse;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Responses.Count);
            Assert.AreEqual(2, result.Responses[0].Item.Count);
            Assert.IsTrue(result.Responses[0].Item.ContainsKey("Name"));
            Assert.IsTrue(result.Responses[0].Item.ContainsKey("Email"));
            Assert.IsFalse(result.Responses[0].Item.ContainsKey("Id"));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemRequestAndReturnAllNewSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Update))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()),
                updateItemAsync: (_, _) => Task.FromResult(new UpdateItemResponse
                {
                    Attributes = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") },
                        { "Name", AttributeValueFactory.CreateS("UpdatedName") }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                ReturnValues = ReturnValue.ALL_NEW
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as UpdateItemResponse;

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Attributes);
            Assert.IsTrue(result.Attributes.Count > 0);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithConditionCheckActionSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.ConditionCheck))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.ConditionCheck,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
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
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
                        }
                    }
                }
            };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemRequestForTransientItemSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                transaction.Id,
                DateTime.UtcNow,
                true,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()),
                putItemAsync: (_, _) => Task.FromResult(new PutItemResponse
                {
                    Attributes = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") },
                        { "Name", AttributeValueFactory.CreateS("TestName") }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") },
                    { "Name", AttributeValueFactory.CreateS("TestName") }
                }
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as PutItemResponse;

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithDeleteItemRequestSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Delete));
            var requestService = new MockRequestService();

            var amazonDynamoDB = new MockAmazonDynamoDB();

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                }
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as DeleteItemResponse;

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithGetItemRequestSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Get);
            var requestService = new MockRequestService();

            var amazonDynamoDB = new MockAmazonDynamoDB(
                getItemAsync: (_, _) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") },
                        { "Name", AttributeValueFactory.CreateS("TestName") }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                }
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as GetItemResponse;

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Item);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithRollbackAndRollbackImageSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestId")))
                    .Add("Name", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestName"))));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) },
                                { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("1") }
                            }
                        }
                    }
                }),
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord);

            await store.ReleaseLocksAsync(transaction, true, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemAndAllOldForTransientItemReturnsEmpty()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                true,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Update))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                ReturnValues = ReturnValue.ALL_OLD
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as UpdateItemResponse;

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Attributes.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemAndReturnNoneSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Update))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()),
                updateItemAsync: (_, _) => Task.FromResult(new UpdateItemResponse
                {
                    Attributes = new Dictionary<string, AttributeValue>()
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                ReturnValues = ReturnValue.NONE
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as UpdateItemResponse;

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Attributes.Count);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithNonExistentItemReturnsCorrectState()
        {
            var store = CreateVersionedItemStore();
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>();

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.TransactionStateValue.Exists);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithTransientGetItemDeletesSuccessfully()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Get))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) },
                                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") }
                            }
                        }
                    }
                }),
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, false, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemAndReturnAllNewSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                transaction.Id,
                DateTime.UtcNow,
                true,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()),
                putItemAsync: (_, _) => Task.FromResult(new PutItemResponse
                {
                    Attributes = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") },
                        { "Name", AttributeValueFactory.CreateS("NewName") }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") },
                    { "Name", AttributeValueFactory.CreateS("NewName") }
                },
                ReturnValues = ReturnValue.ALL_NEW
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as PutItemResponse;

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Attributes);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUpdateItemAndAllNewWithAppliedStateReturnsFromBackup()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemRecord = new ItemRecord(
                itemKey,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty
                    .Add("Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestId")))
                    .Add("BackupValue", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("FromBackup"))));
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Update))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Update,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                ReturnValues = ReturnValue.ALL_NEW
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as UpdateItemResponse;

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Attributes);
            Assert.IsTrue(result.Attributes.ContainsKey("BackupValue"));
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithRollbackAndDeleteActionSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Delete))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) }
                            }
                        }
                    }
                }),
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, false, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithPutItemWithAttributeExistsConditionSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        "attribute_exists(#id)",
                        ImmutableDictionary<string, string>.Empty.Add("#id", "Id"),
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                }
            };

            var result = await store.AcquireLocksAsync(transaction, request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncWithPutActionReturnsItems()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { "Value", AttributeValueFactory.CreateS("TestValue") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithPutItemAndReturnNoneSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                false,
                transaction.Id,
                DateTime.UtcNow,
                true,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()),
                putItemAsync: (_, _) => Task.FromResult(new PutItemResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                ReturnValues = ReturnValue.NONE
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as PutItemResponse;

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithRollbackAndTransientDeleteSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Delete))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") },
                                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) },
                                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") }
                            }
                        }
                    }
                }),
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, true, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncWithRollbackAndNoTransactionIdSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
                        }
                    }
                }),
                transactWriteItemsAsync: (_, _) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var rollbackImages = ImmutableDictionary<ItemKey, ItemRecord>.Empty;

            await store.ReleaseLocksAsync(transaction, true, rollbackImages, CancellationToken.None);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncWithGetActionReturnsEmptyList()
        {
            var itemKey = CreateItemKey();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Get))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Get,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (_, _) => Task.FromResult(new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new ItemResponse
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
                        }
                    }
                }));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new GetItemRequest { TableName = "TestTable" };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateWithTransientAndNotAppliedReturnsEmptyRecord()
        {
            var store = CreateVersionedItemStore();
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") },
                { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("tx-123") },
                { ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("1") }
            };

            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.ItemResponse.AttributeValues.Count);
            Assert.IsTrue(result.TransactionStateValue.IsTransient);
            Assert.IsFalse(result.TransactionStateValue.IsApplied);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithDeleteItemAndReturnNoneSucceeds()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Delete));
            var requestService = new MockRequestService();

            var amazonDynamoDB = new MockAmazonDynamoDB();

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                },
                ReturnValues = ReturnValue.NONE
            };

            var applyRequest = new ApplyRequestRequest(
                transaction,
                request,
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None) as DeleteItemResponse;

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Attributes.Count);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncWithConditionCheckActionReturnsEmptyList()
        {
            var itemKey = CreateItemKey();
            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.ConditionCheck))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.ConditionCheck,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB();

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);
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
                                { "Id", AttributeValueFactory.CreateS("TestId") }
                            }
                        }
                    }
                }
            };

            var result = await store.GetItemsToBackupAsync(request, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }






        [TestMethod]
        public async Task AcquireLocksAsyncWithDeleteActionGuessesExistsTrue()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Delete))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Delete,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Update);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var deleteRequest = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                }
            };

            var result = await store.AcquireLocksAsync(transaction, deleteRequest, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithGetActionGuessesExistsTrue()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Get))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Get,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Update);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var getRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") }
                }
            };

            var result = await store.AcquireLocksAsync(transaction, getRequest, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task AcquireLocksAsyncWithPutAndAttributeExistsGuessesExistsTrue()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new LockedItemRequestAction(itemKey, 0, RequestAction.Put))),
                getItemRequestDetailsAsync: (_, _) => Task.FromResult(ImmutableList.Create(
                    new ItemRequestDetail(
                        itemKey,
                        RequestAction.Put,
                        "attribute_exists(Id)",
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty))));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Update);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var putRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", AttributeValueFactory.CreateS("TestId") },
                    { "Data", AttributeValueFactory.CreateS("TestData") }
                },
                ConditionExpression = "attribute_exists(Id)"
            };

            var result = await store.AcquireLocksAsync(transaction, putRequest, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncGetItemForDeletedItemReturnsEmpty()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Delete, isApplied: true);

            var requestService = new MockRequestService();
            var store = CreateVersionedItemStore(requestService: requestService);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new GetItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(GetItemResponse));
            var getItemResponse = (GetItemResponse)result;
            Assert.AreEqual(0, getItemResponse.Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncGetItemWithProjectionNoAttributeNamesFiltersCorrectly()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Get, isApplied: true);

            var itemRecord = new ItemRecord(itemKey, new Dictionary<string, ImmutableAttributeValue>
            {
                { "Field1", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("Value1")) },
                { "Field2", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("Value2")) },
                { "Field3", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("Value3")) }
            }.ToImmutableDictionary());

            var requestService = new MockRequestService();
            var store = CreateVersionedItemStore(requestService: requestService);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new GetItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    },
                    ProjectionExpression = "Field1, Field2"
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(GetItemResponse));
            var getItemResponse = (GetItemResponse)result;
            Assert.AreEqual(2, getItemResponse.Item.Count);
            Assert.IsTrue(getItemResponse.Item.ContainsKey("Field1"));
            Assert.IsTrue(getItemResponse.Item.ContainsKey("Field2"));
            Assert.IsFalse(getItemResponse.Item.ContainsKey("Field3"));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncGetItemWithExpressionAttributeNamesResolvesCorrectly()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Get, isApplied: true);

            var itemRecord = new ItemRecord(itemKey, new Dictionary<string, ImmutableAttributeValue>
            {
                { "status", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("active")) },
                { "data", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test")) }
            }.ToImmutableDictionary());

            var requestService = new MockRequestService();
            var store = CreateVersionedItemStore(requestService: requestService);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new GetItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    },
                    ProjectionExpression = "#s",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#s", "status" }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(GetItemResponse));
            var getItemResponse = (GetItemResponse)result;
            Assert.AreEqual(1, getItemResponse.Item.Count);
            Assert.IsTrue(getItemResponse.Item.ContainsKey("status"));
            Assert.IsFalse(getItemResponse.Item.ContainsKey("data"));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncWithUnsupportedRequestTypeThrowsNotSupported()
        {
            var transaction = CreateTransaction();
            var requestService = new MockRequestService();
            var store = CreateVersionedItemStore(requestService: requestService);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new BatchGetItemRequest(),
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await store.ApplyRequestAsync(applyRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncOtherTransactionWithoutRollbackThrowsArgument()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var owningTransaction = CreateTransaction("tx-owner");
            var itemTransactionState = CreateItemTransactionState(itemKey, owningTransaction, RequestAction.Put, isApplied: false);

            var requestService = new MockRequestService();
            var store = CreateVersionedItemStore(requestService: requestService);

            await Assert.ThrowsExceptionAsync<ArgumentException>(async () =>
            {
                await store.ReleaseLocksAsync(
                    new TransactionId(transaction.Id),
                    new TransactionId(owningTransaction.Id),
                    ImmutableList.Create(itemKey),
                    false,
                    ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                    ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                    CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactGetItemsSkipsGetForDeleteAction()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Delete, isApplied: true);

            var requestService = new MockRequestService();
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(0, request.TransactItems.Count);
                    return Task.FromResult(new TransactGetItemsResponse { Responses = new List<ItemResponse>() });
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactGetItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactGetItemsResponse));
            var response = (TransactGetItemsResponse)result;
            Assert.AreEqual(1, response.Responses.Count);
            Assert.AreEqual(0, response.Responses[0].Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactGetItemsSkipsGetForTransientGet()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                true,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Get));

            var requestService = new MockRequestService();
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(0, request.TransactItems.Count);
                    return Task.FromResult(new TransactGetItemsResponse { Responses = new List<ItemResponse>() });
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactGetItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactGetItemsResponse));
            var response = (TransactGetItemsResponse)result;
            Assert.AreEqual(1, response.Responses.Count);
            Assert.AreEqual(0, response.Responses[0].Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactGetItemsSkipsGetWhenBackupExists()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Put, isApplied: true);
            var itemRecord = new ItemRecord(itemKey, new Dictionary<string, ImmutableAttributeValue>
            {
                { "Field1", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("Value1")) }
            }.ToImmutableDictionary());

            var requestService = new MockRequestService();
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(0, request.TransactItems.Count);
                    return Task.FromResult(new TransactGetItemsResponse { Responses = new List<ItemResponse>() });
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactGetItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactGetItemsResponse));
            var response = (TransactGetItemsResponse)result;
            Assert.AreEqual(1, response.Responses.Count);
            Assert.AreEqual(1, response.Responses[0].Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactGetItemsPerformsGetWhenRequired()
        {
            var itemKey = CreateItemKey();
            var transaction = CreateTransaction();
            var itemTransactionState = CreateItemTransactionState(itemKey, transaction, RequestAction.Put, isApplied: true);

            var requestService = new MockRequestService();
            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (request, _) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse
                            {
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    { "Field1", AttributeValueFactory.CreateS("Value1") }
                                }
                            }
                        }
                    });
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactGetItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactGetItemsResponse));
            var response = (TransactGetItemsResponse)result;
            Assert.AreEqual(1, response.Responses.Count);
            Assert.AreEqual(1, response.Responses[0].Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncDeleteItemReturnsAllOldWhenTransient()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                true,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Delete));

            var store = CreateVersionedItemStore();

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new DeleteItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    },
                    ReturnValues = ReturnValue.ALL_OLD
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(DeleteItemResponse));
            var response = (DeleteItemResponse)result;
            Assert.AreEqual(0, response.Attributes.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncDeleteItemReturnsAllOldWithBackup()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Delete));

            var itemRecord = new ItemRecord(
                itemKey,
                new Dictionary<string, ImmutableAttributeValue>
                {
                    { "Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestId")) },
                    { "Name", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestName")) }
                }.ToImmutableDictionary());

            var store = CreateVersionedItemStore();

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new DeleteItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    },
                    ReturnValues = ReturnValue.ALL_OLD
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(DeleteItemResponse));
            var response = (DeleteItemResponse)result;
            Assert.AreEqual(2, response.Attributes.Count);
            Assert.IsTrue(response.Attributes.ContainsKey("Id"));
            Assert.IsTrue(response.Attributes.ContainsKey("Name"));
        }

        [TestMethod]
        public void GetItemRecordAndTransactionStateReturnsNullWhenNotConditionalCheckFailed()
        {
            var itemKey = CreateItemKey();
            var item = new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS("TestId") },
                { "Name", AttributeValueFactory.CreateS("TestName") }
            };

            var store = CreateVersionedItemStore();
            var result = store.GetItemRecordAndTransactionState(itemKey, item);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.ItemResponse);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public async Task ApplyRequestAsyncThrowsForUnsupportedRequestType()
        {
            var transaction = CreateTransaction();
            var store = CreateVersionedItemStore();

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new ScanRequest { TableName = "TestTable" },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await store.ApplyRequestAsync(applyRequest, CancellationToken.None);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesConditionCheckAttributeNotExists()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                true,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.ConditionCheck));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.ConditionCheck, "attribute_not_exists(Id)", 
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                },
                                ConditionExpression = "attribute_not_exists(Id)"
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesConditionCheckAttributeExists()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.ConditionCheck));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.ConditionCheck, "attribute_exists(Id)",
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                },
                                ConditionExpression = "attribute_exists(Id)"
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public async Task ApplyRequestAsyncTransactWriteItemsThrowsForUnsupportedConditionExpression()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.ConditionCheck));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.ConditionCheck, "size(Name) > :val",
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB();

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                },
                                ConditionExpression = "size(Name) > :val",
                                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                                {
                                    { ":val", AttributeValueFactory.CreateN("5") }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await store.ApplyRequestAsync(applyRequest, CancellationToken.None);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task ApplyRequestAsyncTransactWriteItemsThrowsWhenItemNotTransientForAttributeNotExists()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var itemRequestDetail = new ItemRequestDetail(itemKey, RequestAction.Put, "attribute_not_exists(Id)",
                ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(itemRequestDetail));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "Name", AttributeValueFactory.CreateS("TestName") }
                                },
                                ConditionExpression = "attribute_not_exists(Id)"
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            await store.ApplyRequestAsync(applyRequest, CancellationToken.None);
        }

        [TestMethod]
        public async Task GetItemsToBackupAsyncReturnsEmptyForNonMutatingRequests()
        {
            var itemKey = CreateItemKey();
            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Get, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var store = CreateVersionedItemStore(requestService: requestService);

            var result = await store.GetItemsToBackupAsync(
                new GetItemRequest { TableName = "TestTable", Key = new Dictionary<string, AttributeValue>() },
                CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithBinaryAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var binaryData = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "Data", AttributeValueFactory.CreateB(binaryData) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithBoolAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "IsActive", AttributeValueFactory.CreateBOOL(true) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithBinarySetAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var binarySet = new List<MemoryStream>
            {
                new MemoryStream(new byte[] { 1, 2, 3 }),
                new MemoryStream(new byte[] { 4, 5, 6 })
            };
            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "DataSet", AttributeValueFactory.CreateBS(binarySet) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithNumberSetAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "Numbers", AttributeValueFactory.CreateNS(new List<string> { "1", "2", "3" }) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithStringSetAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "Tags", AttributeValueFactory.CreateSS(new List<string> { "tag1", "tag2", "tag3" }) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithListAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "Items", AttributeValueFactory.CreateL(new List<AttributeValue>
                                    {
                                        AttributeValueFactory.CreateS("item1"),
                                        AttributeValueFactory.CreateN("123")
                                    }) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithMapAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "Metadata", AttributeValueFactory.CreateM(new Dictionary<string, AttributeValue>
                                    {
                                        { "key1", AttributeValueFactory.CreateS("value1") },
                                        { "key2", AttributeValueFactory.CreateN("456") }
                                    }) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncTransactWriteItemsHandlesPutWithNullAttribute()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Put));

            var requestService = new MockRequestService(
                getItemRequestDetailsAsync: (req, ct) =>
                {
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty.Add(
                        new ItemRequestDetail(itemKey, RequestAction.Put, null,
                            ImmutableDictionary<string, string>.Empty, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)));
                });

            var amazonDynamoDBKeyService = new MockAmazonDynamoDBKeyService(
                createKeyMapAsync: (tableName, item, ct) =>
                {
                    return Task.FromResult(new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }.ToImmutableDictionary());
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(
                requestService: requestService,
                amazonDynamoDBKeyService: amazonDynamoDBKeyService,
                amazonDynamoDB: amazonDynamoDB);

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new TransactWriteItemsRequest
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
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { "OptionalField", AttributeValueFactory.CreateNULL(true) }
                                }
                            }
                        }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(TransactWriteItemsResponse));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncGetItemRequestUsesProjectionExpression()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Get));

            var itemRecord = new ItemRecord(
                itemKey,
                new Dictionary<string, ImmutableAttributeValue>
                {
                    { "Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestId")) },
                    { "Name", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestName")) },
                    { "Email", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("test@example.com")) }
                }.ToImmutableDictionary());

            var store = CreateVersionedItemStore();

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new GetItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    },
                    ProjectionExpression = "#name, Email",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#name", "Name" }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord));

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(GetItemResponse));
            var response = (GetItemResponse)result;
            Assert.AreEqual(2, response.Item.Count);
            Assert.IsTrue(response.Item.ContainsKey("Name"));
            Assert.IsTrue(response.Item.ContainsKey("Email"));
            Assert.IsFalse(response.Item.ContainsKey("Id"));
        }

        [TestMethod]
        public async Task ApplyRequestAsyncGetItemRequestForDeletedItemReturnsEmpty()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Delete));

            var store = CreateVersionedItemStore();

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new GetItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(GetItemResponse));
            var response = (GetItemResponse)result;
            Assert.AreEqual(0, response.Item.Count);
        }

        [TestMethod]
        public async Task ApplyRequestAsyncGetItemRequestForTransientGetReturnsEmpty()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                true,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Get));

            var store = CreateVersionedItemStore();

            var applyRequest = new ApplyRequestRequest(
                transaction,
                new GetItemRequest
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", AttributeValueFactory.CreateS("TestId") }
                    }
                },
                0,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty);

            var result = await store.ApplyRequestAsync(applyRequest, CancellationToken.None);

            Assert.IsInstanceOfType(result, typeof(GetItemResponse));
            var response = (GetItemResponse)result;
            Assert.AreEqual(0, response.Item.Count);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public async Task ReleaseLocksAsyncThrowsForRollbackWithoutBackupImage()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (txn, ct) =>
                {
                    return Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty.Add(
                        new LockedItemRequestAction(itemKey, 0, RequestAction.Update)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse
                            {
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) },
                                    { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString()) },
                                    { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("1") }
                                }
                            }
                        }
                    });
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            await store.ReleaseLocksAsync(
                transaction,
                true,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                CancellationToken.None);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncHandlesRollbackWithBackupImage()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                transaction.Id,
                DateTime.UtcNow,
                false,
                true,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));

            var itemRecord = new ItemRecord(
                itemKey,
                new Dictionary<string, ImmutableAttributeValue>
                {
                    { "Id", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("TestId")) },
                    { "Name", ImmutableAttributeValue.Create(AttributeValueFactory.CreateS("OldName")) }
                }.ToImmutableDictionary());

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (txn, ct) =>
                {
                    return Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty.Add(
                        new LockedItemRequestAction(itemKey, 0, RequestAction.Update)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse
                            {
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) },
                                    { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString()) },
                                    { ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("1") }
                                }
                            }
                        }
                    });
                },
                transactWriteItemsAsync: (req, ct) => Task.FromResult(new TransactWriteItemsResponse()));

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            await store.ReleaseLocksAsync(
                transaction,
                true,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey, itemRecord),
                CancellationToken.None);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncHandlesDeleteAlreadyCarriedOut()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (txn, ct) =>
                {
                    return Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty.Add(
                        new LockedItemRequestAction(itemKey, 0, RequestAction.Delete)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse
                            {
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    { "Id", AttributeValueFactory.CreateS("TestId") },
                                    { ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id) },
                                    { ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString()) }
                                }
                            }
                        }
                    });
                },
                transactWriteItemsAsync: (req, ct) =>
                {
                    Assert.AreEqual(1, req.TransactItems.Count);
                    Assert.IsNotNull(req.TransactItems[0].Delete);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            await store.ReleaseLocksAsync(
                transaction,
                false,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                CancellationToken.None);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncHandlesAlreadyCompletedOperation()
        {
            var transaction = CreateTransaction();
            var itemKey = CreateItemKey();

            var requestService = new MockRequestService(
                getItemRequestActionsAsync: (txn, ct) =>
                {
                    return Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty.Add(
                        new LockedItemRequestAction(itemKey, 0, RequestAction.Update)));
                });

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactGetItemsAsync: (req, ct) =>
                {
                    return Task.FromResult(new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse
                            {
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    { "Id", AttributeValueFactory.CreateS("TestId") }
                                }
                            }
                        }
                    });
                },
                transactWriteItemsAsync: (req, ct) =>
                {
                    Assert.AreEqual(0, req.TransactItems.Count);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(requestService: requestService, amazonDynamoDB: amazonDynamoDB);

            await store.ReleaseLocksAsync(
                transaction,
                false,
                ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                CancellationToken.None);
        }

        [TestMethod]
        public async Task ReleaseLocksAsyncFromOtherTransactionHandlesRollbackWithTransactionIdNull()
        {
            var transaction = CreateTransaction();
            var owningTransactionId = new TransactionId("tx-owner");
            var itemKey = CreateItemKey();
            var itemTransactionState = new ItemTransactionState(
                itemKey,
                true,
                null,
                DateTime.UtcNow,
                false,
                false,
                new LockedItemRequestAction(itemKey, 0, RequestAction.Update));

            var amazonDynamoDB = new MockAmazonDynamoDB(
                transactWriteItemsAsync: (req, ct) =>
                {
                    Assert.AreEqual(1, req.TransactItems.Count);
                    Assert.IsNotNull(req.TransactItems[0].ConditionCheck);
                    return Task.FromResult(new TransactWriteItemsResponse());
                });

            var store = CreateVersionedItemStore(amazonDynamoDB: amazonDynamoDB);

            await store.ReleaseLocksAsync(
                transaction.GetId(),
                owningTransactionId,
                ImmutableList<ItemKey>.Empty.Add(itemKey),
                true,
                ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey, itemTransactionState),
                ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                CancellationToken.None);
        }
    }
}
