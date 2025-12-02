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
    public sealed class AmazonDynamoDBWithTransactionsTests
    {
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

            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public void Dispose() { }
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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

        private static AmazonDynamoDBWithTransactions CreateService(
            IOptionsSnapshot<AmazonDynamoDBOptions>? options = null,
            MockAmazonDynamoDB? amazonDynamoDB = null,
            MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>? unCommittedService = null,
            MockIsolatedGetItemService<CommittedIsolationLevelServiceType>? committedService = null,
            MockTransactionStore? transactionStore = null,
            MockVersionedItemStore? versionedItemStore = null,
            MockItemImageStore? itemImageStore = null,
            MockRequestService? requestService = null,
            MockTransactionServiceEvents? transactionServiceEvents = null,
            MockFullyAppliedRequestService? fullyAppliedRequestService = null)
        {
            var defaultOptions = new AmazonDynamoDBOptions
            {
                TransactionStaleDuration = TimeSpan.FromMinutes(5),
                QuickTransactionsEnabled = false,
                TransactWriteItemCountMaxValue = 100,
                TransactGetItemCountMaxValue = 100
            };

            var optionsSnapshot = options ?? new MockOptionsSnapshot<AmazonDynamoDBOptions>(defaultOptions);

            return new AmazonDynamoDBWithTransactions(
                optionsSnapshot,
                amazonDynamoDB ?? new MockAmazonDynamoDB(),
                unCommittedService ?? new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>(),
                committedService ?? new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>(),
                transactionStore ?? new MockTransactionStore(),
                versionedItemStore ?? new MockVersionedItemStore(),
                itemImageStore ?? new MockItemImageStore(),
                requestService ?? new MockRequestService(),
                transactionServiceEvents ?? new MockTransactionServiceEvents(),
                fullyAppliedRequestService ?? new MockFullyAppliedRequestService());
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

        [TestMethod]
        public void PaginatorsPropertyReturnsPaginatorsFromUnderlyingClient()
        {
            var mockPaginators = new object() as IDynamoDBv2PaginatorFactory;
            var mockDynamoDB = new MockAmazonDynamoDB { Paginators = mockPaginators! };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            var result = service.Paginators;

            Assert.AreEqual(mockPaginators, result);
        }

        [TestMethod]
        public void ConfigPropertyReturnsConfigFromUnderlyingClient()
        {
            var mockConfig = new object() as IClientConfig;
            var mockDynamoDB = new MockAmazonDynamoDB { Config = mockConfig! };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            var result = service.Config;

            Assert.AreEqual(mockConfig, result);
        }

        [TestMethod]
        public async Task BeginTransactionAsyncReturnsTransactionId()
        {
            var transactionAdded = false;
            var mockTransactionStore = new MockTransactionStore
            {
                AddAsyncFunc = ct =>
                {
                    transactionAdded = true;
                    return Task.FromResult(Transaction.CreateNew());
                }
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            var result = await service.BeginTransactionAsync(CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsTrue(transactionAdded);
        }

        [TestMethod]
        public async Task ResumeTransactionAsyncReturnsTransactionId()
        {
            var transactionId = new TransactionId("test-id");
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(Transaction.CreateNew())
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            var result = await service.ResumeTransactionAsync(transactionId, CancellationToken.None);

            Assert.AreEqual(transactionId, result);
        }

        [TestMethod]
        public async Task ResumeTransactionAsyncInvokesEventHandler()
        {
            var eventInvoked = false;
            var transactionId = new TransactionId("test-id");
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(Transaction.CreateNew())
            };
            var mockEvents = new MockTransactionServiceEvents
            {
                OnResumeTransactionFinishAsync = (id, ct) =>
                {
                    eventInvoked = true;
                    return Task.CompletedTask;
                }
            };
            var service = CreateService(transactionStore: mockTransactionStore, transactionServiceEvents: mockEvents);

            await service.ResumeTransactionAsync(transactionId, CancellationToken.None);

            Assert.IsTrue(eventInvoked);
        }

        [TestMethod]
        public async Task GetItemAsyncWithoutTransactionCallsIsolatedService()
        {
            var called = false;
            var mockService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new GetItemResponse { Item = new Dictionary<string, AttributeValue>() });
                }
            };
            var service = CreateService(unCommittedService: mockService);
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task GetItemAsyncWithIsolationLevelCallsCorrectService()
        {
            var uncommittedCalled = false;
            var committedCalled = false;
            var mockUncommittedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    uncommittedCalled = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };
            var mockCommittedService = new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    committedCalled = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };
            var service = CreateService(unCommittedService: mockUncommittedService, committedService: mockCommittedService);
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.GetItemAsync(IsolationLevel.UnCommitted, request, CancellationToken.None);
            Assert.IsTrue(uncommittedCalled);
            Assert.IsFalse(committedCalled);

            uncommittedCalled = false;
            await service.GetItemAsync(IsolationLevel.Committed, request, CancellationToken.None);
            Assert.IsFalse(uncommittedCalled);
            Assert.IsTrue(committedCalled);
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithIsolationLevelCallsCorrectService()
        {
            var committedCalled = false;
            var mockCommittedService = new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    committedCalled = true;
                    return Task.FromResult(new TransactGetItemsResponse());
                }
            };
            var service = CreateService(committedService: mockCommittedService);
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>()
            };

            await service.TransactGetItemsAsync(IsolationLevel.Committed, request, CancellationToken.None);

            Assert.IsTrue(committedCalled);
        }

        [TestMethod]
        public async Task PutItemAsyncWithoutTransactionDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new PutItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>()
            };

            await service.PutItemAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithoutTransactionDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.UpdateItemAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithoutTransactionDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DeleteItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.DeleteItemAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                BatchGetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new BatchGetItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new BatchGetItemRequest();

            await service.BatchGetItemAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                BatchWriteItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new BatchWriteItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new BatchWriteItemRequest();

            await service.BatchWriteItemAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task QueryAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                QueryAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new QueryResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new QueryRequest();

            await service.QueryAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ScanAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ScanResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ScanRequest();

            await service.ScanAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public void DisposeDoesNotThrow()
        {
            var service = CreateService();
            service.Dispose();
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new GetItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenAttributesToGetIsPopulated()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                AttributesToGet = new List<string> { "attr1" }
            };

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("Legacy"));
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new PutItemRequest
            {
                TableName = null,
                Item = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenExpectedIsPopulated()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>(),
                Expected = new Dictionary<string, ExpectedAttributeValue>
                {
                    { "attr1", new ExpectedAttributeValue() }
                }
            };

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("Legacy"));
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new UpdateItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenAttributeUpdatesIsPopulated()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
                {
                    { "attr1", new AttributeValueUpdate() }
                }
            };

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("Legacy"));
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new DeleteItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

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
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", new AttributeValue { S = "test" } }
                            }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
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

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenTableNameIsNull()
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
                            Item = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void CombineJoinsExpressionsWithAnd()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine("expr1", "expr2", "expr3");
            Assert.AreEqual("expr1 AND expr2 AND expr3", result);
        }

        [TestMethod]
        public void CombineIgnoresNullAndWhitespace()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine("expr1", null, "", "  ", "expr2");
            Assert.AreEqual("expr1 AND expr2", result);
        }

        [TestMethod]
        public void CombineReturnsEmptyWhenAllNullOrWhitespace()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine(null, "", "  ");
            Assert.AreEqual("", result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionReturnsTrueForAttributeNotExists()
        {
            var conditionCheck = new ConditionCheck
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                ConditionExpression = "attribute_not_exists(Id)"
            };

            var result = AmazonDynamoDBWithTransactionsTestHelper.IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionReturnsFalseForDifferentExpression()
        {
            var conditionCheck = new ConditionCheck
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                ConditionExpression = "attribute_exists(Id)"
            };

            var result = AmazonDynamoDBWithTransactionsTestHelper.IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task GetItemAsyncWithInvalidIsolationLevelThrows()
        {
            var service = CreateService();
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await Assert.ThrowsExceptionAsync<ArgumentException>(async () =>
            {
                await service.GetItemAsync((IsolationLevel)999, request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task CommitTransactionAsyncWithActiveStateUpdatesTransactionToCommitting()
        {
            var transaction = Transaction.CreateNew();
            var transactionUpdated = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    transactionUpdated = true;
                    Assert.AreEqual(TransactionState.Committing, txn.State);
                    return Task.FromResult(txn with { State = TransactionState.Committed });
                }
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty)
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);

            await service.CommitTransactionAsync(transaction.GetId(), CancellationToken.None);

            Assert.IsTrue(transactionUpdated);
        }

        [TestMethod]
        public async Task RollbackTransactionAsyncWithActiveStateUpdatesTransactionToRollingBack()
        {
            var transaction = Transaction.CreateNew();
            var transactionUpdated = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    transactionUpdated = true;
                    Assert.AreEqual(TransactionState.RollingBack, txn.State);
                    return Task.FromResult(txn with { State = TransactionState.RolledBack });
                }
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty)
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);

            await service.RollbackTransactionAsync(transaction.GetId(), CancellationToken.None);

            Assert.IsTrue(transactionUpdated);
        }

        [TestMethod]
        public async Task CommitTransactionAsyncWithCommittedStateDoesNotThrow()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            await service.CommitTransactionAsync(transaction.GetId(), CancellationToken.None);
        }

        [TestMethod]
        public async Task RollbackTransactionAsyncWithRolledBackStateDoesNotThrow()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.RolledBack };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            await service.RollbackTransactionAsync(transaction.GetId(), CancellationToken.None);
        }

        [TestMethod]
        public async Task CommitTransactionAsyncWithRolledBackStateThrowsTransactionRolledBackException()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.RolledBack };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            await Assert.ThrowsExceptionAsync<TransactionRolledBackException>(async () =>
            {
                await service.CommitTransactionAsync(transaction.GetId(), CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task RollbackTransactionAsyncWithCommittedStateThrowsTransactionCommittedException()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(async () =>
            {
                await service.RollbackTransactionAsync(transaction.GetId(), CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncProcessesTransactions()
        {
            var transaction1 = Transaction.CreateNew() with { State = TransactionState.Committed };
            var transaction2 = Transaction.CreateNew() with { State = TransactionState.RolledBack };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(transaction1, transaction2)),
                TryRemoveAsyncFunc = (id, duration, ct) => Task.FromResult(true)
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(2, result.Items.Count);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncDoesNotRollbackRecentActiveTransactions()
        {
            var recentTransaction = Transaction.CreateNew() with
            {
                State = TransactionState.Active,
                LastUpdateDateTime = DateTime.UtcNow
            };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(recentTransaction))
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(HouseKeepTransactionAction.None, result.Items[0].Action);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncHandlesCommittingStateByRollingBack()
        {
            var committingTransaction = Transaction.CreateNew() with { State = TransactionState.Committing };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(committingTransaction)),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(committingTransaction with { State = TransactionState.RolledBack }),
                UpdateAsyncFunc = (txn, ct) => Task.FromResult(txn with { State = TransactionState.RolledBack })
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty)
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(HouseKeepTransactionAction.RolledBack, result.Items[0].Action);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncHandlesRollingBackStateByRollingBack()
        {
            var rollingBackTransaction = Transaction.CreateNew() with { State = TransactionState.RollingBack };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(rollingBackTransaction)),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(rollingBackTransaction with { State = TransactionState.RolledBack }),
                UpdateAsyncFunc = (txn, ct) => Task.FromResult(txn with { State = TransactionState.RolledBack })
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty)
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(HouseKeepTransactionAction.RolledBack, result.Items[0].Action);
        }

        [TestMethod]
        public async Task PutItemAsyncWithTransactionIdAppendsRequest()
        {
            var transaction = Transaction.CreateNew();
            var appendCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) =>
                {
                    appendCalled = true;
                    return Task.FromResult(txn);
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new PutItemResponse())
            };
            var service = CreateService(transactionStore: mockTransactionStore, versionedItemStore: mockVersionedItemStore);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.PutItemAsync(transaction.GetId(), request, CancellationToken.None);

            Assert.IsTrue(appendCalled);
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithTransactionIdAppendsRequest()
        {
            var transaction = Transaction.CreateNew();
            var appendCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) =>
                {
                    appendCalled = true;
                    return Task.FromResult(txn);
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new UpdateItemResponse())
            };
            var service = CreateService(transactionStore: mockTransactionStore, versionedItemStore: mockVersionedItemStore);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.UpdateItemAsync(transaction.GetId(), request, CancellationToken.None);

            Assert.IsTrue(appendCalled);
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithTransactionIdAppendsRequest()
        {
            var transaction = Transaction.CreateNew();
            var appendCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) =>
                {
                    appendCalled = true;
                    return Task.FromResult(txn);
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new DeleteItemResponse())
            };
            var service = CreateService(transactionStore: mockTransactionStore, versionedItemStore: mockVersionedItemStore);
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.DeleteItemAsync(transaction.GetId(), request, CancellationToken.None);

            Assert.IsTrue(appendCalled);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithSingleItemProcessesRequest()
        {
            var transaction = Transaction.CreateNew();
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) => Task.FromResult(txn)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new TransactWriteItemsResponse())
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                versionedItemStore: mockVersionedItemStore,
                requestService: mockRequestService);
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var result = await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task GetItemAsyncWithTransactionIdProcessesRequest()
        {
            var transaction = Transaction.CreateNew();
            var processRequestCalled = false;
            var expectedItem = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) => Task.FromResult(txn)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) =>
                {
                    processRequestCalled = true;
                    return Task.FromResult<AmazonWebServiceResponse>(new GetItemResponse { Item = expectedItem });
                }
            };
            var service = CreateService(transactionStore: mockTransactionStore, versionedItemStore: mockVersionedItemStore);
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            var result = await service.GetItemAsync(transaction.GetId(), request, CancellationToken.None);

            Assert.IsTrue(processRequestCalled);
            Assert.AreEqual(expectedItem, result.Item);
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithTransactionIdProcessesRequest()
        {
            var transaction = Transaction.CreateNew();
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) => Task.FromResult(txn)
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new TransactGetItemsResponse())
            };
            var service = CreateService(transactionStore: mockTransactionStore, versionedItemStore: mockVersionedItemStore);
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var result = await service.TransactGetItemsAsync(transaction.GetId(), request, CancellationToken.None);

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task PutItemAsyncWithCommittedTransactionThrowsTransactionCommittedException()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(async () =>
            {
                await service.PutItemAsync(transaction.GetId(), request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithRolledBackTransactionThrowsTransactionRolledBackException()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.RolledBack };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await Assert.ThrowsExceptionAsync<TransactionRolledBackException>(async () =>
            {
                await service.UpdateItemAsync(transaction.GetId(), request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithCommittedTransactionThrowsTransactionCommittedException()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(async () =>
            {
                await service.DeleteItemAsync(transaction.GetId(), request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetItemAsyncWithCommittedTransactionThrowsTransactionCommittedException()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(async () =>
            {
                await service.GetItemAsync(transaction.GetId(), request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithIsolationLevelCallsIsolatedService()
        {
            var called = false;
            var mockService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new TransactGetItemsResponse());
                }
            };
            var service = CreateService(unCommittedService: mockService);
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            await service.TransactGetItemsAsync(IsolationLevel.UnCommitted, request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public void ValidateBatchGetItemRequestThrowsNotSupported()
        {
            var request = new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>()
            };

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public void ValidateQueryRequestThrowsNotSupported()
        {
            var request = new QueryRequest
            {
                TableName = "TestTable"
            };

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public void ValidateScanRequestThrowsNotSupported()
        {
            var request = new ScanRequest
            {
                TableName = "TestTable"
            };

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncHandlesCompletedTransactionException()
        {
            var committingTransaction = Transaction.CreateNew() with { State = TransactionState.Committing };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(committingTransaction)),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(committingTransaction with { State = TransactionState.Committed }),
                UpdateAsyncFunc = (txn, ct) => throw new TransactionCompletedException()
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
        }

        [TestMethod]
        public void CombineWithNoExpressionsReturnsEmpty()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine();
            Assert.AreEqual("", result);
        }

        [TestMethod]
        public void CombineWithSingleExpressionReturnsSame()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine("expr1");
            Assert.AreEqual("expr1", result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionWithDifferentKeyReturnsFalse()
        {
            var conditionCheck = new ConditionCheck
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                ConditionExpression = "attribute_not_exists(OtherId)"
            };

            var result = AmazonDynamoDBWithTransactionsTestHelper.IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task CommitTransactionAsyncWithCommittingStateProcessesTransaction()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committing };
            var updateCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    updateCalled = true;
                    return Task.FromResult(txn with { State = TransactionState.Committed });
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                ReleaseLocksAsync2Func = (txn, rollback, records, ct) => Task.CompletedTask
            };
            var mockItemImageStore = new MockItemImageStore
            {
                DeleteItemImagesAsync1Func = (txn, ct) => Task.CompletedTask
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                versionedItemStore: mockVersionedItemStore,
                itemImageStore: mockItemImageStore);

            await service.CommitTransactionAsync(transaction.GetId(), CancellationToken.None);

            Assert.IsTrue(updateCalled);
        }

        [TestMethod]
        public async Task RollbackTransactionAsyncWithRollingBackStateProcessesTransaction()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.RollingBack };
            var updateCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    updateCalled = true;
                    return Task.FromResult(txn with { State = TransactionState.RolledBack });
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                ReleaseLocksAsync2Func = (txn, rollback, records, ct) => Task.CompletedTask
            };
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) => Task.FromResult(ImmutableList<ItemRecord>.Empty),
                DeleteItemImagesAsync1Func = (txn, ct) => Task.CompletedTask
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                versionedItemStore: mockVersionedItemStore,
                itemImageStore: mockItemImageStore);

            await service.RollbackTransactionAsync(transaction.GetId(), CancellationToken.None);

            Assert.IsTrue(updateCalled);
        }

        [TestMethod]
        public async Task GetItemAsyncWithTableNameAndKeyDelegatesToRequestOverload()
        {
            var called = false;
            var mockService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    Assert.AreEqual("TestTable", req.TableName);
                    return Task.FromResult(new GetItemResponse());
                }
            };
            var service = CreateService(unCommittedService: mockService);

            await service.GetItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task GetItemAsyncWithConsistentReadDelegatesToRequestOverload()
        {
            var called = false;
            var mockService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    Assert.IsTrue(req.ConsistentRead);
                    return Task.FromResult(new GetItemResponse());
                }
            };
            var service = CreateService(unCommittedService: mockService);

            await service.GetItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, true, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task PutItemAsyncWithTableNameAndItemDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new PutItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.PutItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task PutItemAsyncWithReturnValuesDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    Assert.AreEqual(ReturnValue.ALL_OLD, req.ReturnValues);
                    return Task.FromResult(new PutItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.PutItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, ReturnValue.ALL_OLD, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithTableNameAndKeyDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DeleteItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.DeleteItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithReturnValuesDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    Assert.AreEqual(ReturnValue.ALL_OLD, req.ReturnValues);
                    return Task.FromResult(new DeleteItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.DeleteItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, ReturnValue.ALL_OLD, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithTableNameAndKeyDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.UpdateItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, new Dictionary<string, AttributeValueUpdate>(), CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithReturnValuesDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    Assert.AreEqual(ReturnValue.ALL_NEW, req.ReturnValues);
                    return Task.FromResult(new UpdateItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.UpdateItemAsync("TestTable", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } }, new Dictionary<string, AttributeValueUpdate>(), ReturnValue.ALL_NEW, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWithDictionaryDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                BatchGetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new BatchGetItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.BatchGetItemAsync(new Dictionary<string, KeysAndAttributes>(), CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWithReturnConsumedCapacityDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                BatchGetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    Assert.AreEqual(ReturnConsumedCapacity.TOTAL, req.ReturnConsumedCapacity);
                    return Task.FromResult(new BatchGetItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.BatchGetItemAsync(new Dictionary<string, KeysAndAttributes>(), ReturnConsumedCapacity.TOTAL, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWithDictionaryDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                BatchWriteItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new BatchWriteItemResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.BatchWriteItemAsync(new Dictionary<string, List<WriteRequest>>(), CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncWithTableNameAndAttributesDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ScanAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ScanResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ScanAsync("TestTable", new List<string> { "attr1" }, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncWithScanFilterDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ScanAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ScanResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ScanAsync("TestTable", new Dictionary<string, Condition>(), CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncWithAttributesAndFilterDelegatesToRequestOverload()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ScanAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ScanResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ScanAsync("TestTable", new List<string> { "attr1" }, new Dictionary<string, Condition>(), CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithMultipleItemsBatchesRequests()
        {
            var transaction = Transaction.CreateNew();
            var commitCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                AddAsyncFunc = ct =>
                {
                    return Task.FromResult(transaction);
                },
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) => Task.FromResult(txn),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    if (txn.State == TransactionState.Committing)
                    {
                        commitCalled = true;
                        return Task.FromResult(txn with { State = TransactionState.Committed });
                    }
                    return Task.FromResult(txn);
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(new TransactWriteItemsResponse()),
                ReleaseLocksAsync2Func = (txn, rollback, records, ct) => Task.CompletedTask
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockItemImageStore = new MockItemImageStore
            {
                DeleteItemImagesAsync1Func = (txn, ct) => Task.CompletedTask
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                versionedItemStore: mockVersionedItemStore,
                requestService: mockRequestService,
                itemImageStore: mockItemImageStore);
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem { Put = new Put { TableName = "TestTable", Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } } } },
                    new TransactWriteItem { Put = new Put { TableName = "TestTable", Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "2" } } } } }
                }
            };

            var result = await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsTrue(commitCalled);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncRollsBackOnException()
        {
            var transaction = Transaction.CreateNew();
            var rollbackCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                AddAsyncFunc = ct => Task.FromResult(transaction),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) => throw new InvalidOperationException("Test exception"),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    if (txn.State == TransactionState.RollingBack)
                    {
                        rollbackCalled = true;
                        return Task.FromResult(txn with { State = TransactionState.RolledBack });
                    }
                    return Task.FromResult(txn);
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ReleaseLocksAsync2Func = (txn, rollback, records, ct) => Task.CompletedTask
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) => Task.FromResult(ImmutableList<ItemRecord>.Empty),
                DeleteItemImagesAsync1Func = (txn, ct) => Task.CompletedTask
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                versionedItemStore: mockVersionedItemStore,
                requestService: mockRequestService,
                itemImageStore: mockItemImageStore);
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem { Put = new Put { TableName = "TestTable", Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } } } }
                }
            };

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });

            Assert.IsTrue(rollbackCalled);
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithoutTransactionBatchesAndCommits()
        {
            var transaction = Transaction.CreateNew();
            var commitCalled = false;
            var mockTransactionStore = new MockTransactionStore
            {
                AddAsyncFunc = ct => Task.FromResult(transaction),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction),
                AppendRequestAsyncFunc = (txn, req, ct) => Task.FromResult(txn),
                UpdateAsyncFunc = (txn, ct) =>
                {
                    if (txn.State == TransactionState.Committing)
                    {
                        commitCalled = true;
                        return Task.FromResult(txn with { State = TransactionState.Committed });
                    }
                    return Task.FromResult(txn);
                }
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                AcquireLocksAsyncFunc = (txn, req, ct) => Task.FromResult(ImmutableDictionary<ItemKey, ItemTransactionState>.Empty),
                ApplyRequestAsyncFunc = (req, ct) => Task.FromResult<AmazonWebServiceResponse>(
                    new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } } }
                        }
                    }),
                ReleaseLocksAsync2Func = (txn, rollback, records, ct) => Task.CompletedTask
            };
            var mockRequestService = new MockRequestService
            {
                GetItemRequestActionsAsyncFunc = (txn, ct) => Task.FromResult(ImmutableList<LockedItemRequestAction>.Empty)
            };
            var mockItemImageStore = new MockItemImageStore
            {
                DeleteItemImagesAsync1Func = (txn, ct) => Task.CompletedTask
            };
            var service = CreateService(
                transactionStore: mockTransactionStore,
                versionedItemStore: mockVersionedItemStore,
                requestService: mockRequestService,
                itemImageStore: mockItemImageStore);
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var result = await service.TransactGetItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Responses.Count);
            Assert.IsTrue(commitCalled);
        }

        [TestMethod]
        public async Task ExecuteStatementAsyncThrowsNotImplementedException()
        {
            var service = CreateService();
            var request = new ExecuteStatementRequest();

            await Assert.ThrowsExceptionAsync<NotImplementedException>(async () =>
            {
                await service.ExecuteStatementAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task BatchExecuteStatementAsyncThrowsNotImplementedException()
        {
            var service = CreateService();
            var request = new BatchExecuteStatementRequest();

            await Assert.ThrowsExceptionAsync<NotImplementedException>(async () =>
            {
                await service.BatchExecuteStatementAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task ExecuteTransactionAsyncThrowsNotImplementedException()
        {
            var service = CreateService();
            var request = new ExecuteTransactionRequest();

            await Assert.ThrowsExceptionAsync<NotImplementedException>(async () =>
            {
                await service.ExecuteTransactionAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public void DetermineServiceOperationEndpointThrowsNotImplementedException()
        {
            var service = CreateService();
            var request = new GetItemRequest();

            Assert.ThrowsException<NotImplementedException>(() =>
            {
                service.DetermineServiceOperationEndpoint(request);
            });
        }
    }

    public static class AmazonDynamoDBWithTransactionsTestHelper
    {
        public static void ValidateRequest(AmazonDynamoDBRequest request)
        {
            switch (request)
            {
                case GetItemRequest getItemRequest:
                    ValidateRequest(getItemRequest);
                    return;
                case PutItemRequest putItemRequest:
                    ValidateRequest(putItemRequest);
                    return;
                case UpdateItemRequest updateItemRequest:
                    ValidateRequest(updateItemRequest);
                    return;
                case DeleteItemRequest deleteItemRequest:
                    ValidateRequest(deleteItemRequest);
                    return;
                case TransactWriteItemsRequest transactWriteItemsRequest:
                    ValidateRequest(transactWriteItemsRequest);
                    return;
                case TransactGetItemsRequest transactGetItemsRequest:
                    ValidateRequest(transactGetItemsRequest);
                    return;
                default:
                    throw new NotSupportedException();
            }
        }

        public static void ValidateRequest(GetItemRequest request)
        {
            if (request.AttributesToGet?.Count > 0)
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(PutItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }
        }

        public static void ValidateRequest(UpdateItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0) || (request.AttributeUpdates?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(DeleteItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(TransactGetItemsRequest request)
        {
            if (request.TransactItems != null && request.TransactItems.Any(v => string.IsNullOrWhiteSpace(v.Get.TableName)))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.TransactItems != null && request.TransactItems.Any(v => v.Get.Key?.Count == 0))
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(TransactWriteItemsRequest request)
        {
            if (request.TransactItems != null && request.TransactItems.Any(v => string.IsNullOrWhiteSpace(v.ConditionCheck?.TableName ?? v.Delete?.TableName ?? v.Put?.TableName ?? v.Update?.TableName)))
            {
                throw new InvalidOperationException("TableName must not be null");
            }
        }

        public static string Combine(params string?[] expressions)
        {
            return string.Join(" AND ", expressions.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e?.Trim()));
        }

        public static bool IsSupportedConditionExpression(ConditionCheck conditionCheck, string conditionExpressionFunction)
        {
            return conditionCheck.Key.Keys.Any(key => conditionCheck.ConditionExpression == $"{conditionExpressionFunction}({key})");
        }
    }
}
