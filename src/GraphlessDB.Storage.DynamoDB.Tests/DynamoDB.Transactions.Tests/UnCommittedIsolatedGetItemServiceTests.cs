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
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class UnCommittedIsolatedGetItemServiceTests
    {
        private const string TableName = "TestTable";

        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            private readonly Func<GetItemRequest, CancellationToken, Task<GetItemResponse>>? _getItemAsync;
            private readonly Func<BatchGetItemRequest, CancellationToken, Task<BatchGetItemResponse>>? _batchGetItemAsync;
            private readonly Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>>? _transactGetItemsAsync;

            public MockAmazonDynamoDB(
                Func<GetItemRequest, CancellationToken, Task<GetItemResponse>>? getItemAsync = null,
                Func<BatchGetItemRequest, CancellationToken, Task<BatchGetItemResponse>>? batchGetItemAsync = null,
                Func<TransactGetItemsRequest, CancellationToken, Task<TransactGetItemsResponse>>? transactGetItemsAsync = null)
            {
                _getItemAsync = getItemAsync;
                _batchGetItemAsync = batchGetItemAsync;
                _transactGetItemsAsync = transactGetItemsAsync;
            }

            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default)
            {
                return _getItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new GetItemResponse { Item = new Dictionary<string, AttributeValue>() });
            }

            public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default)
            {
                return _batchGetItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new BatchGetItemResponse { Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>() });
            }

            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default)
            {
                return _transactGetItemsAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new TransactGetItemsResponse { Responses = new List<ItemResponse>() });
            }

            public void Dispose() { }
            public Amazon.Runtime.IClientConfig Config => throw new NotImplementedException();
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
            public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.DynamoDBv2.Model.IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.Runtime.Endpoints.IEndpointProvider EndpointProvider => throw new NotImplementedException();
            public Task DetermineServiceOperationEndpointAsync(Amazon.Runtime.AmazonWebServiceRequest request) => throw new NotImplementedException();
            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(Amazon.Runtime.AmazonWebServiceRequest request) => throw new NotImplementedException();
        }

        private sealed class MockVersionedItemStore : IVersionedItemStore
        {
            private readonly Func<Dictionary<string, AttributeValue>, Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>>? _getItemRecordAndTransactionState;

            public MockVersionedItemStore(
                Func<Dictionary<string, AttributeValue>, Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>>? getItemRecordAndTransactionState = null)
            {
                _getItemRecordAndTransactionState = getItemRecordAndTransactionState;
            }

            public Tuple<Dictionary<string, AttributeValue>, TransactionStateValue> GetItemRecordAndTransactionState(Dictionary<string, AttributeValue> item)
            {
                return _getItemRecordAndTransactionState?.Invoke(item)
                    ?? new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                        item,
                        new TransactionStateValue(true, null, null, false, false));
            }

            public ItemResponseAndTransactionState<ItemRecord> GetItemRecordAndTransactionState(ItemKey itemKey, Dictionary<string, AttributeValue> item) => throw new NotImplementedException();
            public Task<System.Collections.Immutable.ImmutableList<ItemRecord>> GetItemsToBackupAsync(AmazonDynamoDBRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<System.Collections.Immutable.ImmutableDictionary<ItemKey, ItemTransactionState>> AcquireLocksAsync(Transaction transaction, AmazonDynamoDBRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<Amazon.Runtime.AmazonWebServiceResponse> ApplyRequestAsync(ApplyRequestRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task ReleaseLocksAsync(Transaction transaction, bool rollback, System.Collections.Immutable.ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task ReleaseLocksAsync(TransactionId id, TransactionId owningTransactionId, System.Collections.Immutable.ImmutableList<ItemKey> itemKeys, bool rollback, System.Collections.Immutable.ImmutableDictionary<ItemKey, ItemTransactionState> itemTransactionStatesByKey, System.Collections.Immutable.ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey, CancellationToken cancellationToken) => throw new NotImplementedException();
        }

        private static Dictionary<string, AttributeValue> CreateKey(string id)
        {
            return new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS(id) }
            };
        }

        private static Dictionary<string, AttributeValue> CreateItem(string id, string value)
        {
            return new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS(id) },
                { "Value", AttributeValueFactory.CreateS(value) }
            };
        }

        [TestMethod]
        public async Task GetItemAsyncReturnsItemWithoutTransaction()
        {
            var key = CreateKey("test-id");
            var item = CreateItem("test-id", "test-value");

            var mockClient = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) => Task.FromResult(new GetItemResponse { Item = item }));

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = key
            };

            var response = await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Item);
            Assert.AreEqual(2, response.Item.Count);
            Assert.IsTrue(response.Item.ContainsKey("Id"));
            Assert.IsTrue(response.Item.ContainsKey("Value"));
        }

        [TestMethod]
        public async Task GetItemAsyncWithConsistentReadPassesConsistentReadToClient()
        {
            var key = CreateKey("test-id");
            var item = CreateItem("test-id", "test-value");
            bool? capturedConsistentRead = null;

            var mockClient = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) =>
                {
                    capturedConsistentRead = request.ConsistentRead;
                    return Task.FromResult(new GetItemResponse { Item = item });
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = key,
                ConsistentRead = true
            };

            await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsTrue(capturedConsistentRead.HasValue);
            Assert.IsTrue(capturedConsistentRead.Value);
        }

        [TestMethod]
        public async Task GetItemAsyncWithProjectionExpressionAddsItemAttributeNames()
        {
            var key = CreateKey("test-id");
            var item = CreateItem("test-id", "test-value");
            string? capturedProjectionExpression = null;
            Dictionary<string, string>? capturedExpressionAttributeNames = null;

            var mockClient = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) =>
                {
                    capturedProjectionExpression = request.ProjectionExpression;
                    capturedExpressionAttributeNames = request.ExpressionAttributeNames;
                    return Task.FromResult(new GetItemResponse { Item = item });
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = key,
                ProjectionExpression = "#name",
                ExpressionAttributeNames = new Dictionary<string, string> { { "#name", "Value" } }
            };

            await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedProjectionExpression);
            Assert.IsNotNull(capturedExpressionAttributeNames);
            Assert.IsTrue(capturedProjectionExpression.Contains("#_TxId"));
            Assert.IsTrue(capturedProjectionExpression.Contains("#_TxD"));
            Assert.IsTrue(capturedProjectionExpression.Contains("#_TxT"));
            Assert.IsTrue(capturedProjectionExpression.Contains("#_TxA"));
            Assert.IsTrue(capturedExpressionAttributeNames.ContainsKey("#_TxId"));
            Assert.IsTrue(capturedExpressionAttributeNames.ContainsKey("#name"));
        }

        [TestMethod]
        public async Task GetItemAsyncWithEmptyItemReturnsEmptyItem()
        {
            var key = CreateKey("test-id");
            var emptyItem = new Dictionary<string, AttributeValue>();

            var mockClient = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) => Task.FromResult(new GetItemResponse { Item = emptyItem }));

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = key
            };

            var response = await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Item);
            Assert.AreEqual(0, response.Item.Count);
        }

        [TestMethod]
        public async Task GetItemAsyncWithTransientItemNotAppliedReturnsEmptyItem()
        {
            var key = CreateKey("test-id");
            var item = CreateItem("test-id", "test-value");

            var mockClient = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) => Task.FromResult(new GetItemResponse { Item = item }));

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    item,
                    new TransactionStateValue(true, "tx-123", null, true, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = key
            };

            var response = await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Item);
            Assert.AreEqual(0, response.Item.Count);
        }

        [TestMethod]
        public async Task GetItemAsyncWithTransientItemAppliedReturnsItem()
        {
            var key = CreateKey("test-id");
            var item = CreateItem("test-id", "test-value");

            var mockClient = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) => Task.FromResult(new GetItemResponse { Item = item }));

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    item,
                    new TransactionStateValue(true, "tx-123", null, true, true)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = key
            };

            var response = await service.GetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Item);
            Assert.AreEqual(2, response.Item.Count);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncReturnsItems()
        {
            var key1 = CreateKey("test-id-1");
            var key2 = CreateKey("test-id-2");
            var item1 = CreateItem("test-id-1", "value-1");
            var item2 = CreateItem("test-id-2", "value-2");

            var mockClient = new MockAmazonDynamoDB(
                batchGetItemAsync: (request, ct) =>
                {
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            { TableName, new List<Dictionary<string, AttributeValue>> { item1, item2 } }
                        }
                    };
                    return Task.FromResult(response);
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    {
                        TableName,
                        new KeysAndAttributes
                        {
                            Keys = new List<Dictionary<string, AttributeValue>> { key1, key2 }
                        }
                    }
                }
            };

            var response = await service.BatchGetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Responses);
            Assert.AreEqual(1, response.Responses.Count);
            Assert.IsTrue(response.Responses.ContainsKey(TableName));
            Assert.AreEqual(2, response.Responses[TableName].Count);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWithProjectionExpressionAddsItemAttributeNames()
        {
            var key1 = CreateKey("test-id-1");
            var item1 = CreateItem("test-id-1", "value-1");
            KeysAndAttributes? capturedKeysAndAttributes = null;

            var mockClient = new MockAmazonDynamoDB(
                batchGetItemAsync: (request, ct) =>
                {
                    capturedKeysAndAttributes = request.RequestItems[TableName];
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            { TableName, new List<Dictionary<string, AttributeValue>> { item1 } }
                        }
                    };
                    return Task.FromResult(response);
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    {
                        TableName,
                        new KeysAndAttributes
                        {
                            Keys = new List<Dictionary<string, AttributeValue>> { key1 },
                            ProjectionExpression = "#name",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#name", "Value" } }
                        }
                    }
                }
            };

            await service.BatchGetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedKeysAndAttributes);
            Assert.IsNotNull(capturedKeysAndAttributes.ProjectionExpression);
            Assert.IsTrue(capturedKeysAndAttributes.ProjectionExpression.Contains("#_TxId"));
        }

        [TestMethod]
        public async Task BatchGetItemAsyncPassesUnprocessedKeys()
        {
            var key1 = CreateKey("test-id-1");
            var item1 = CreateItem("test-id-1", "value-1");
            var unprocessedKey = CreateKey("unprocessed-id");

            var mockClient = new MockAmazonDynamoDB(
                batchGetItemAsync: (request, ct) =>
                {
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            { TableName, new List<Dictionary<string, AttributeValue>> { item1 } }
                        },
                        UnprocessedKeys = new Dictionary<string, KeysAndAttributes>
                        {
                            { TableName, new KeysAndAttributes { Keys = new List<Dictionary<string, AttributeValue>> { unprocessedKey } } }
                        }
                    };
                    return Task.FromResult(response);
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    { TableName, new KeysAndAttributes { Keys = new List<Dictionary<string, AttributeValue>> { key1 } } }
                }
            };

            var response = await service.BatchGetItemAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.UnprocessedKeys);
            Assert.AreEqual(1, response.UnprocessedKeys.Count);
            Assert.IsTrue(response.UnprocessedKeys.ContainsKey(TableName));
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncReturnsItems()
        {
            var key1 = CreateKey("test-id-1");
            var item1 = CreateItem("test-id-1", "value-1");

            var mockClient = new MockAmazonDynamoDB(
                transactGetItemsAsync: (request, ct) =>
                {
                    var response = new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = item1 }
                        }
                    };
                    return Task.FromResult(response);
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = TableName,
                            Key = key1
                        }
                    }
                }
            };

            var response = await service.TransactGetItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Responses);
            Assert.AreEqual(1, response.Responses.Count);
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithProjectionExpressionAddsItemAttributeNames()
        {
            var key1 = CreateKey("test-id-1");
            var item1 = CreateItem("test-id-1", "value-1");
            Get? capturedGet = null;

            var mockClient = new MockAmazonDynamoDB(
                transactGetItemsAsync: (request, ct) =>
                {
                    capturedGet = request.TransactItems[0].Get;
                    var response = new TransactGetItemsResponse
                    {
                        Responses = new List<ItemResponse>
                        {
                            new ItemResponse { Item = item1 }
                        }
                    };
                    return Task.FromResult(response);
                });

            var mockVersionedItemStore = new MockVersionedItemStore(
                getItemRecordAndTransactionState: (i) => new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                    i,
                    new TransactionStateValue(true, null, null, false, false)));

            var service = new UnCommittedIsolatedGetItemService(
                mockClient,
                mockVersionedItemStore);

            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = TableName,
                            Key = key1,
                            ProjectionExpression = "#name",
                            ExpressionAttributeNames = new Dictionary<string, string> { { "#name", "Value" } }
                        }
                    }
                }
            };

            await service.TransactGetItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedGet);
            Assert.IsNotNull(capturedGet.ProjectionExpression);
            Assert.IsTrue(capturedGet.ProjectionExpression.Contains("#_TxId"));
        }
    }
}
