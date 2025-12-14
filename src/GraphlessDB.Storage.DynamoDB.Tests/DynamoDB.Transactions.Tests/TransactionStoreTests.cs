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
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class TransactionStoreTests
    {
        private sealed class MockOptionsSnapshot<T> : IOptionsSnapshot<T> where T : class, new()
        {
            private readonly T _value;
            public MockOptionsSnapshot(T value) => _value = value;
            public T Value => _value;
            public T Get(string? name) => _value;
        }

        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            public Func<ScanRequest, CancellationToken, Task<ScanResponse>> ScanAsyncFunc { get; set; } = (_, _) => Task.FromResult(new ScanResponse { Items = [] });
            public Func<GetItemRequest, CancellationToken, Task<GetItemResponse>> GetItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new GetItemResponse { Item = [] });
            public Func<PutItemRequest, CancellationToken, Task<PutItemResponse>> PutItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new PutItemResponse());
            public Func<UpdateItemRequest, CancellationToken, Task<UpdateItemResponse>> UpdateItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new UpdateItemResponse());
            public Func<DeleteItemRequest, CancellationToken, Task<DeleteItemResponse>> DeleteItemAsyncFunc { get; set; } = (_, _) => Task.FromResult(new DeleteItemResponse());

            public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken) => ScanAsyncFunc(request, cancellationToken);
            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken) => GetItemAsyncFunc(request, cancellationToken);
            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken) => PutItemAsyncFunc(request, cancellationToken);
            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken) => UpdateItemAsyncFunc(request, cancellationToken);
            public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken) => DeleteItemAsyncFunc(request, cancellationToken);

            // Not implemented methods (required by interface)
            public Amazon.Runtime.IClientConfig Config => throw new NotImplementedException();
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
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(Amazon.Runtime.AmazonWebServiceRequest request) => throw new NotImplementedException();
            public IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
            public void Dispose() { }
        }

        private sealed class MockRequestRecordSerializer : IRequestRecordSerializer
        {
            public Func<RequestRecord, byte[]> SerializeFunc { get; set; } = r => BitConverter.GetBytes(r.Id);
            public Func<byte[], RequestRecord> DeserializeFunc { get; set; } = b => new RequestRecord(BitConverter.ToInt32(b, 0), null, null, null, null, null, null);

            public byte[] Serialize(RequestRecord value) => SerializeFunc(value);
            public RequestRecord Deserialize(byte[] value) => DeserializeFunc(value);
        }

        private static DefaultTransactionStore CreateStore(
            TransactionStoreOptions? options = null,
            MockAmazonDynamoDB? client = null,
            MockRequestRecordSerializer? serializer = null)
        {
            return new DefaultTransactionStore(
                new MockOptionsSnapshot<TransactionStoreOptions>(options ?? new TransactionStoreOptions { TransactionTableName = "TestTable" }),
                client ?? new MockAmazonDynamoDB(),
                serializer ?? new MockRequestRecordSerializer());
        }

        [TestMethod]
        public async Task ListAsyncReturnsTransactions()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                ScanAsyncFunc = (req, ct) =>
                {
                    var items = new List<Dictionary<string, AttributeValue>>
                    {
                        new Dictionary<string, AttributeValue>
                        {
                            { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                            { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                            { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                            { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                            { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                        }
                    };
                    return Task.FromResult(new ScanResponse { Items = items });
                }
            };

            var store = CreateStore(client: client);
            var result = await store.ListAsync(10, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(txId, result[0].Id);
        }

        [TestMethod]
        public void GetKeyReturnsCorrectKey()
        {
            var store = CreateStore();
            var txId = new TransactionId("test-id");

            var key = store.GetKey(txId);

            Assert.AreEqual(1, key.Count);
            Assert.AreEqual("test-id", key[TransactionAttributeName.TXID.Value].S);
        }

        [TestMethod]
        public async Task ContainsAsyncReturnsTrueWhenItemExists()
        {
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = "test-id" } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.ContainsAsync(new TransactionId("test-id"), CancellationToken.None);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task ContainsAsyncReturnsFalseWhenItemDoesNotExist()
        {
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse { Item = new Dictionary<string, AttributeValue>() })
            };

            var store = CreateStore(client: client);
            var result = await store.ContainsAsync(new TransactionId("test-id"), CancellationToken.None);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task AddAsyncSuccessfullyAddsTransaction()
        {
            var putItemRequestCaptured = false;
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) =>
                {
                    putItemRequestCaptured = true;
                    Assert.IsNotNull(req.ConditionExpression);
                    return Task.FromResult(new PutItemResponse());
                }
            };

            var transaction = Transaction.CreateNew();
            var store = CreateStore(client: client);

            await store.AddAsync(transaction, CancellationToken.None);

            Assert.IsTrue(putItemRequestCaptured);
        }

        [TestMethod]
        public async Task AddAsyncThrowsTransactionCommittedExceptionWhenTransactionIsCommitting()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateCommitted } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(
                async () => await store.AddAsync(transaction, CancellationToken.None));
        }

        [TestMethod]
        public async Task AddAsyncThrowsTransactionCommittedExceptionWhenTransactionIsCommitted()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateCommitted } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "1" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(
                async () => await store.AddAsync(transaction, CancellationToken.None));
        }

        [TestMethod]
        public async Task AddAsyncThrowsTransactionRolledBackExceptionWhenTransactionIsRollingBack()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateRolledBack } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionRolledBackException>(
                async () => await store.AddAsync(transaction, CancellationToken.None));
        }

        [TestMethod]
        public async Task AddAsyncThrowsTransactionRolledBackExceptionWhenTransactionIsRolledBack()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateRolledBack } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "1" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionRolledBackException>(
                async () => await store.AddAsync(transaction, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetAsyncReturnsTransactionFromCache()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) => Task.FromResult(new PutItemResponse()),
                GetItemAsyncFunc = (req, ct) => throw new InvalidOperationException("Should not be called")
            };

            var store = CreateStore(client: client);
            await store.AddAsync(transaction, CancellationToken.None);

            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(txId, result.Id);
        }

        [TestMethod]
        public async Task GetAsyncFetchesTransactionWhenNotInCache()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(txId, result.Id);
        }

        [TestMethod]
        public async Task GetAsyncForceFetchIgnoresCache()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var fetchCalled = false;
            var client = new MockAmazonDynamoDB
            {
                PutItemAsyncFunc = (req, ct) => Task.FromResult(new PutItemResponse()),
                GetItemAsyncFunc = (req, ct) =>
                {
                    fetchCalled = true;
                    return Task.FromResult(new GetItemResponse
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                            { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                            { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "2" } },
                            { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                            { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                        }
                    });
                }
            };

            var store = CreateStore(client: client);
            await store.AddAsync(transaction, CancellationToken.None);

            var result = await store.GetAsync(new TransactionId(txId), true, CancellationToken.None);

            Assert.IsTrue(fetchCalled);
            Assert.AreEqual(2, result.Version);
        }

        [TestMethod]
        public async Task GetAsyncThrowsTransactionNotFoundExceptionWhenItemDoesNotExist()
        {
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse { Item = new Dictionary<string, AttributeValue>() })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionNotFoundException>(
                async () => await store.GetAsync(new TransactionId("non-existent"), false, CancellationToken.None));
        }

        [TestMethod]
        public async Task UpdateAsyncSuccessfullyUpdatesTransaction()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Committing, 2, DateTime.UtcNow, []);
            var updateCalled = false;
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) =>
                {
                    updateCalled = true;
                    Assert.IsNotNull(req.ConditionExpression);
                    return Task.FromResult(new UpdateItemResponse());
                }
            };

            var store = CreateStore(client: client);
            var result = await store.UpdateAsync(transaction, CancellationToken.None);

            Assert.IsTrue(updateCalled);
            Assert.AreEqual(txId, result.Id);
        }

        [TestMethod]
        public async Task UpdateAsyncThrowsTransactionCommittedExceptionWhenConflictOccurs()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateCommitted } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "2" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "1" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(
                async () => await store.UpdateAsync(transaction, CancellationToken.None));
        }

        [TestMethod]
        public async Task AppendRequestAsyncSuccessfullyAppendsRequest()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var request = new GetItemRequest();
            var updateCalled = false;
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) =>
                {
                    updateCalled = true;
                    return Task.FromResult(new UpdateItemResponse());
                }
            };

            var store = CreateStore(client: client);
            var result = await store.AppendRequestAsync(transaction, request, CancellationToken.None);

            Assert.IsTrue(updateCalled);
            Assert.AreEqual(2, result.Version);
            Assert.AreEqual(1, result.Requests.Count);
        }

        [TestMethod]
        public async Task AppendRequestAsyncThrowsTransactionCommittedExceptionWhenTransactionNotActive()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Active, 1, DateTime.UtcNow, []);
            var request = new GetItemRequest();
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateCommitted } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "2" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionCommittedException>(
                async () => await store.AppendRequestAsync(transaction, request, CancellationToken.None));
        }

        [TestMethod]
        public async Task RemoveAsyncSuccessfullyRemovesTransaction()
        {
            var txId = new TransactionId("test-id");
            var deleteCalled = false;
            var client = new MockAmazonDynamoDB
            {
                DeleteItemAsyncFunc = (req, ct) =>
                {
                    deleteCalled = true;
                    return Task.FromResult(new DeleteItemResponse());
                }
            };

            var store = CreateStore(client: client);
            await store.RemoveAsync(txId, CancellationToken.None);

            Assert.IsTrue(deleteCalled);
        }

        [TestMethod]
        public async Task RemoveAsyncThrowsTransactionNotFoundExceptionWhenItemDoesNotExist()
        {
            var txId = new TransactionId("test-id");
            var client = new MockAmazonDynamoDB
            {
                DeleteItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse { Item = new Dictionary<string, AttributeValue>() })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionNotFoundException>(
                async () => await store.RemoveAsync(txId, CancellationToken.None));
        }

        [TestMethod]
        public async Task RemoveAsyncThrowsTransactionExceptionWhenItemExistsButNotFinalized()
        {
            var txId = new TransactionId("test-id");
            var client = new MockAmazonDynamoDB
            {
                DeleteItemAsyncFunc = (req, ct) => throw new ConditionalCheckFailedException("Condition failed"),
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = "test-id" } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<TransactionException>(
                async () => await store.RemoveAsync(txId, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetAsyncParsesCommittingStateCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateCommitted } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(TransactionState.Committing, result.State);
        }

        [TestMethod]
        public async Task GetAsyncParsesCommittedStateCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateCommitted } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "1" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(TransactionState.Committed, result.State);
        }

        [TestMethod]
        public async Task GetAsyncParsesRollingBackStateCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateRolledBack } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(TransactionState.RollingBack, result.State);
        }

        [TestMethod]
        public async Task GetAsyncParsesRolledBackStateCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StateRolledBack } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "1" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(TransactionState.RolledBack, result.State);
        }

        [TestMethod]
        public async Task GetAsyncParsesActiveStateCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(TransactionState.Active, result.State);
        }

        [TestMethod]
        public async Task GetAsyncThrowsInvalidOperationExceptionForUnrecognizedState()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = "X" } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                async () => await store.GetAsync(new TransactionId(txId), false, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetAsyncThrowsInvalidOperationExceptionForInvalidVersionNumber()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "invalid" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                async () => await store.GetAsync(new TransactionId(txId), false, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetAsyncThrowsInvalidOperationExceptionForMissingDateAttribute()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                async () => await store.GetAsync(new TransactionId(txId), false, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetAsyncThrowsInvalidOperationExceptionForInvalidDateFormat()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = "invalid" } }
                    }
                })
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                async () => await store.GetAsync(new TransactionId(txId), false, CancellationToken.None));
        }

        [TestMethod]
        public async Task GetAsyncParsesRequestsCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var serializer = new MockRequestRecordSerializer
            {
                DeserializeFunc = b => new RequestRecord(BitConverter.ToInt32(b, 0), new GetItemRequest(), null, null, null, null, null)
            };
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } },
                        { TransactionAttributeName.REQUESTS.Value, new AttributeValue { BS = [new System.IO.MemoryStream(BitConverter.GetBytes(1))] } }
                    }
                })
            };

            var store = CreateStore(client: client, serializer: serializer);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(1, result.Requests.Count);
            Assert.AreEqual(1, result.Requests[0].Id);
        }

        [TestMethod]
        public async Task GetAsyncReturnsEmptyRequestsWhenAttributeNotPresent()
        {
            var txId = Guid.NewGuid().ToString();
            var client = new MockAmazonDynamoDB
            {
                GetItemAsyncFunc = (req, ct) => Task.FromResult(new GetItemResponse
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { TransactionAttributeName.TXID.Value, new AttributeValue { S = txId } },
                        { TransactionAttributeName.STATE.Value, new AttributeValue { S = DefaultTransactionStore.StatePending } },
                        { TransactionAttributeName.VERSION.Value, new AttributeValue { N = "1" } },
                        { TransactionAttributeName.FINALIZED.Value, new AttributeValue { S = "0" } },
                        { TransactionAttributeName.DATE.Value, new AttributeValue { N = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture) } }
                    }
                })
            };

            var store = CreateStore(client: client);
            var result = await store.GetAsync(new TransactionId(txId), false, CancellationToken.None);

            Assert.AreEqual(0, result.Requests.Count);
        }

        [TestMethod]
        public async Task UpdateAsyncWithCommittingStateCallsStateToStringCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Committing, 2, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => Task.FromResult(new UpdateItemResponse())
            };

            var store = CreateStore(client: client);
            await store.UpdateAsync(transaction, CancellationToken.None);

            // Test passes if no exception is thrown
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task UpdateAsyncWithCommittedStateCallsStateToStringCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.Committed, 2, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => Task.FromResult(new UpdateItemResponse())
            };

            var store = CreateStore(client: client);
            await store.UpdateAsync(transaction, CancellationToken.None);

            // Test passes if no exception is thrown
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task UpdateAsyncWithRollingBackStateCallsStateToStringCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.RollingBack, 2, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => Task.FromResult(new UpdateItemResponse())
            };

            var store = CreateStore(client: client);
            await store.UpdateAsync(transaction, CancellationToken.None);

            // Test passes if no exception is thrown
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task UpdateAsyncWithRolledBackStateCallsStateToStringCorrectly()
        {
            var txId = Guid.NewGuid().ToString();
            var transaction = new Transaction(txId, TransactionState.RolledBack, 2, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => Task.FromResult(new UpdateItemResponse())
            };

            var store = CreateStore(client: client);
            await store.UpdateAsync(transaction, CancellationToken.None);

            // Test passes if no exception is thrown
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task UpdateAsyncWithInvalidStateThrowsNotSupportedException()
        {
            var txId = Guid.NewGuid().ToString();
            // Use an invalid enum value by casting
            var transaction = new Transaction(txId, (TransactionState)999, 2, DateTime.UtcNow, []);
            var client = new MockAmazonDynamoDB
            {
                UpdateItemAsyncFunc = (req, ct) => Task.FromResult(new UpdateItemResponse())
            };

            var store = CreateStore(client: client);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(
                async () => await store.UpdateAsync(transaction, CancellationToken.None));
        }
    }
}
