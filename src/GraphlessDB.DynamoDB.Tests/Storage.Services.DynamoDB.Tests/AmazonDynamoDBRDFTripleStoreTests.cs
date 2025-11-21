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
using Amazon.DynamoDBv2.Model;
using GraphlessDB;
using GraphlessDB.DynamoDB;
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.DynamoDB;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Storage.Services.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBRDFTripleStoreTests
    {
        private const string TableName = "TestTable";

        private sealed class MockLogger : ILogger<AmazonDynamoDBRDFTripleStore>
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
            }
        }

        private sealed class MockRDFTripleStoreConsumedCapacity : IRDFTripleStoreConsumedCapacity
        {
            private RDFTripleStoreConsumedCapacity capacity = RDFTripleStoreConsumedCapacity.None();

            public void AddConsumedCapacity(RDFTripleStoreConsumedCapacity value)
            {
                capacity = new RDFTripleStoreConsumedCapacity(
                    capacity.CapacityUnits + value.CapacityUnits,
                    capacity.ReadCapacityUnits + value.ReadCapacityUnits,
                    capacity.WriteCapacityUnits + value.WriteCapacityUnits);
            }

            public RDFTripleStoreConsumedCapacity GetConsumedCapacity() => capacity;

            public void ResetConsumedCapacity()
            {
                capacity = RDFTripleStoreConsumedCapacity.None();
            }
        }

        private sealed class MockAmazonDynamoDBClient : IAmazonDynamoDBWithTransactions
        {
            private readonly Func<BatchGetItemRequest, CancellationToken, Task<BatchGetItemResponse>>? batchGetItemHandler;
            private readonly Func<BatchWriteItemRequest, CancellationToken, Task<BatchWriteItemResponse>>? batchWriteItemHandler;
            private readonly Func<TransactWriteItemsRequest, CancellationToken, Task<TransactWriteItemsResponse>>? transactWriteItemsHandler;
            private readonly Func<ScanRequest, CancellationToken, Task<ScanResponse>>? scanHandler;
            private readonly Func<QueryRequest, CancellationToken, Task<QueryResponse>>? queryHandler;
            private readonly Func<RunHouseKeepingRequest, CancellationToken, Task<RunHouseKeepingResponse>>? runHouseKeepingHandler;

            public MockAmazonDynamoDBClient(
                Func<BatchGetItemRequest, CancellationToken, Task<BatchGetItemResponse>>? batchGetItemHandler = null,
                Func<BatchWriteItemRequest, CancellationToken, Task<BatchWriteItemResponse>>? batchWriteItemHandler = null,
                Func<TransactWriteItemsRequest, CancellationToken, Task<TransactWriteItemsResponse>>? transactWriteItemsHandler = null,
                Func<ScanRequest, CancellationToken, Task<ScanResponse>>? scanHandler = null,
                Func<QueryRequest, CancellationToken, Task<QueryResponse>>? queryHandler = null,
                Func<RunHouseKeepingRequest, CancellationToken, Task<RunHouseKeepingResponse>>? runHouseKeepingHandler = null)
            {
                this.batchGetItemHandler = batchGetItemHandler;
                this.batchWriteItemHandler = batchWriteItemHandler;
                this.transactWriteItemsHandler = transactWriteItemsHandler;
                this.scanHandler = scanHandler;
                this.queryHandler = queryHandler;
                this.runHouseKeepingHandler = runHouseKeepingHandler;
            }

            public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default)
            {
                if (batchGetItemHandler == null)
                    throw new NotImplementedException();
                return batchGetItemHandler(request, cancellationToken);
            }

            public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default)
            {
                if (batchWriteItemHandler == null)
                    throw new NotImplementedException();
                return batchWriteItemHandler(request, cancellationToken);
            }

            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default)
            {
                if (transactWriteItemsHandler == null)
                    throw new NotImplementedException();
                return transactWriteItemsHandler(request, cancellationToken);
            }

            public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default)
            {
                if (scanHandler == null)
                    throw new NotImplementedException();
                return scanHandler(request, cancellationToken);
            }

            public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
            {
                if (queryHandler == null)
                    throw new NotImplementedException();
                return queryHandler(request, cancellationToken);
            }

            public Task<RunHouseKeepingResponse> RunHouseKeepingAsync(RunHouseKeepingRequest request, CancellationToken cancellationToken)
            {
                if (runHouseKeepingHandler == null)
                    throw new NotImplementedException();
                return runHouseKeepingHandler(request, cancellationToken);
            }

            #region Unused IAmazonDynamoDB Members
            public Amazon.Runtime.IClientConfig Config => throw new NotImplementedException();
            public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, Amazon.DynamoDBv2.ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, Amazon.DynamoDBv2.ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, Amazon.DynamoDBv2.ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public void Dispose() { }
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(Amazon.Runtime.AmazonWebServiceRequest request) => throw new NotImplementedException();
            public Amazon.DynamoDBv2.Model.IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, Amazon.DynamoDBv2.ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactionId> BeginTransactionAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactionId> ResumeTransactionAsync(TransactionId id, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task CommitTransactionAsync(TransactionId id, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task RollbackTransactionAsync(TransactionId id, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(IsolationLevel isolationLevel, GetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(IsolationLevel isolationLevel, BatchGetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(IsolationLevel isolationLevel, TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(TransactionId id, GetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(TransactionId id, PutItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(TransactionId id, UpdateItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(TransactionId id, DeleteItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactionId id, TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactionId id, TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            #endregion
        }

        private sealed class MockOptionsSnapshot : IOptionsSnapshot<RDFTripleStoreOptions>
        {
            private readonly RDFTripleStoreOptions options;

            public MockOptionsSnapshot(RDFTripleStoreOptions options)
            {
                this.options = options;
            }

            public RDFTripleStoreOptions Value => options;

            public RDFTripleStoreOptions Get(string? name) => options;
        }

        private static AmazonDynamoDBRDFTripleStore CreateStore(
            IAmazonDynamoDBWithTransactions? client = null,
            bool trackConsumedCapacity = false)
        {
            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { TrackConsumedCapacity = trackConsumedCapacity });
            var mockClient = client ?? new MockAmazonDynamoDBClient();
            var dataModelMapper = new AmazonDynamoDBRDFTripleItemService();
            var consumedCapacity = new MockRDFTripleStoreConsumedCapacity();
            var logger = new MockLogger();

            return new AmazonDynamoDBRDFTripleStore(options, mockClient, dataModelMapper, consumedCapacity, logger);
        }

        private static RDFTriple CreateTriple(
            string subject = "test-subject",
            string predicate = "test-predicate",
            string indexedObject = "test-indexed",
            string obj = "test-object",
            string partition = "test-partition",
            VersionDetail? versionDetail = null)
        {
            return new RDFTriple(subject, predicate, indexedObject, obj, partition, versionDetail);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncReturnsEmptyForEmptyKeys()
        {
            var store = CreateStore();
            var request = new GetRDFTriplesRequest(TableName, ImmutableList<RDFTripleKey>.Empty, false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(0, response.Items.Count);
            Assert.AreEqual(0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncReturnsSingleItem()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            {
                                TableName,
                                new List<Dictionary<string, AttributeValue>>
                                {
                                    new Dictionary<string, AttributeValue>
                                    {
                                        { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                        { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                        { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                        { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                        { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                                    }
                                }
                            }
                        },
                        ConsumedCapacity = new List<ConsumedCapacity>(),
                        UnprocessedKeys = new Dictionary<string, KeysAndAttributes>()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey(triple.Subject, triple.Predicate);
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsNotNull(response.Items[0]);
            Assert.AreEqual(triple.Subject, response.Items[0]!.Subject);
            Assert.AreEqual(triple.Predicate, response.Items[0]!.Predicate);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncThrowsForUnprocessedKeys()
        {
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>(),
                        ConsumedCapacity = new List<ConsumedCapacity>(),
                        UnprocessedKeys = new Dictionary<string, KeysAndAttributes>
                        {
                            { TableName, new KeysAndAttributes() }
                        }
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey("test-subject", "test-predicate");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await store.GetRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncBatchesLargeRequestsAndAggregatesResults()
        {
            var keys = Enumerable.Range(0, 150).Select(i => new RDFTripleKey($"subject-{i}", "predicate")).ToImmutableList();

            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    var items = request.RequestItems[TableName].Keys
                        .Select(key => new Dictionary<string, AttributeValue>
                        {
                            { "Subject", key["Subject"] },
                            { "Predicate", key["Predicate"] },
                            { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                            { "Object", AttributeValueFactory.CreateS("object") },
                            { "Partition", AttributeValueFactory.CreateS("partition") }
                        })
                        .ToList();

                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            { TableName, items }
                        },
                        ConsumedCapacity = new List<ConsumedCapacity>
                        {
                            new ConsumedCapacity
                            {
                                CapacityUnits = 1.0,
                                ReadCapacityUnits = 1.0,
                                WriteCapacityUnits = 0.0
                            }
                        },
                        UnprocessedKeys = new Dictionary<string, KeysAndAttributes>()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var request = new GetRDFTriplesRequest(TableName, keys, false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(150, response.Items.Count);
            Assert.AreEqual(2.0, response.ConsumedCapacity.CapacityUnits);
            Assert.AreEqual(2.0, response.ConsumedCapacity.ReadCapacityUnits);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncWithTransactionReturnsEmptyForEmptyItems()
        {
            var store = CreateStore();
            var request = new WriteRDFTriplesRequest("token", false, ImmutableList<WriteRDFTriple>.Empty);

            var response = await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncWithTransactionExecutesTransaction()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    var response = new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>
                        {
                            new ConsumedCapacity
                            {
                                CapacityUnits = 2.0,
                                WriteCapacityUnits = 2.0
                            }
                        }
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var response = await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2.0, response.ConsumedCapacity.CapacityUnits);
            Assert.AreEqual(2.0, response.ConsumedCapacity.WriteCapacityUnits);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncWithoutTransactionReturnsEmptyForEmptyItems()
        {
            var store = CreateStore();
            var request = new WriteRDFTriplesRequest("token", true, ImmutableList<WriteRDFTriple>.Empty);

            var response = await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncWithoutTransactionExecutesBatchWrite()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    var response = new BatchWriteItemResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>
                        {
                            new ConsumedCapacity
                            {
                                CapacityUnits = 1.0,
                                WriteCapacityUnits = 1.0
                            }
                        }
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), VersionDetailCondition.None));
            var request = new WriteRDFTriplesRequest("token", true, [deleteRequest]);

            var response = await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1.0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncReturnsResults()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    var response = new ScanResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity
                        {
                            CapacityUnits = 1.0,
                            ReadCapacityUnits = 1.0
                        }
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client, trackConsumedCapacity: true);
            var request = new ScanRDFTriplesRequest(TableName, null, 0, false, false);

            var response = await store.ScanRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsFalse(response.HasNextPage);
            Assert.AreEqual(1.0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncReturnsResults()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    var response = new QueryResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>
                        {
                            { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                            { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) }
                        },
                        ConsumedCapacity = new ConsumedCapacity
                        {
                            CapacityUnits = 1.0,
                            ReadCapacityUnits = 1.0
                        }
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client, trackConsumedCapacity: true);
            var request = new QueryRDFTriplesRequest(TableName, "test-subject", "test-predicate", null, true, 0, false, false);

            var response = await store.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsTrue(response.HasNextPage);
            Assert.AreEqual(1.0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncReturnsResults()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    var response = new QueryResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity
                        {
                            CapacityUnits = 1.0,
                            ReadCapacityUnits = 1.0
                        }
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client, trackConsumedCapacity: true);
            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(TableName, "test-partition", "test-predicate", null, true, 0, false, false);

            var response = await store.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsFalse(response.HasNextPage);
            Assert.AreEqual(1.0, response.ConsumedCapacity.CapacityUnits);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncCallsClient()
        {
            var called = false;
            var client = new MockAmazonDynamoDBClient(
                runHouseKeepingHandler: (request, ct) =>
                {
                    called = true;
                    Assert.AreEqual(100, request.Limit);
                    return Task.FromResult(new RunHouseKeepingResponse(ImmutableList<HouseKeepTransactionResponse>.Empty));
                });

            var store = CreateStore(client);

            await store.RunHouseKeepingAsync(CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWrapsInternalServerErrorException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    throw new InternalServerErrorException("Test error");
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey("subject", "predicate");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBInternalServerErrorException>(async () =>
            {
                await store.GetRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
            Assert.IsInstanceOfType(exception.InnerException, typeof(InternalServerErrorException));
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWrapsProvisionedThroughputExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    throw new ProvisionedThroughputExceededException("Test error");
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey("subject", "predicate");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBThroughputExceededException>(async () =>
            {
                await store.GetRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWrapsRequestLimitExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    throw new RequestLimitExceededException("Test error");
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey("subject", "predicate");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBRequestLimitExceededException>(async () =>
            {
                await store.GetRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWrapsResourceNotFoundException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    throw new ResourceNotFoundException("Test error");
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey("subject", "predicate");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBResourceNotFoundException>(async () =>
            {
                await store.GetRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWrapsInternalServerErrorException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    throw new InternalServerErrorException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), VersionDetailCondition.None));
            var request = new WriteRDFTriplesRequest("token", true, [deleteRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBInternalServerErrorException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWrapsItemCollectionSizeLimitExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    throw new ItemCollectionSizeLimitExceededException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), VersionDetailCondition.None));
            var request = new WriteRDFTriplesRequest("token", true, [deleteRequest]);

            var exception = await Assert.ThrowsExceptionAsync<RequestSizeLimitExceededException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWrapsProvisionedThroughputExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    throw new ProvisionedThroughputExceededException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), VersionDetailCondition.None));
            var request = new WriteRDFTriplesRequest("token", true, [deleteRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBThroughputExceededException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWrapsRequestLimitExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    throw new RequestLimitExceededException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), VersionDetailCondition.None));
            var request = new WriteRDFTriplesRequest("token", true, [deleteRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBRequestLimitExceededException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWrapsResourceNotFoundException()
        {
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    throw new ResourceNotFoundException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), VersionDetailCondition.None));
            var request = new WriteRDFTriplesRequest("token", true, [deleteRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBResourceNotFoundException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWrapsIdempotentParameterMismatchException()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new IdempotentParameterMismatchException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBIdempotentRequestMismatchException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWrapsInternalServerErrorException()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new InternalServerErrorException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBInternalServerErrorException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWrapsProvisionedThroughputExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new ProvisionedThroughputExceededException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBThroughputExceededException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWrapsRequestLimitExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new RequestLimitExceededException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBRequestLimitExceededException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWrapsResourceNotFoundException()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new ResourceNotFoundException("Test error");
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBResourceNotFoundException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWrapsTransactionCanceledException()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    var exception = new TransactionCanceledException("Test error");
                    exception.CancellationReasons = new List<CancellationReason>
                    {
                        new CancellationReason { Code = "ConditionalCheckFailed" }
                    };
                    throw exception;
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest]);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task ScanAsyncWrapsInternalServerErrorException()
        {
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    throw new InternalServerErrorException("Test error");
                });

            var store = CreateStore(client);
            var request = new ScanRDFTriplesRequest(TableName, null, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBInternalServerErrorException>(async () =>
            {
                await store.ScanRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task ScanAsyncWrapsProvisionedThroughputExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    throw new ProvisionedThroughputExceededException("Test error");
                });

            var store = CreateStore(client);
            var request = new ScanRDFTriplesRequest(TableName, null, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBThroughputExceededException>(async () =>
            {
                await store.ScanRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task ScanAsyncWrapsRequestLimitExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    throw new RequestLimitExceededException("Test error");
                });

            var store = CreateStore(client);
            var request = new ScanRDFTriplesRequest(TableName, null, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBRequestLimitExceededException>(async () =>
            {
                await store.ScanRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task ScanAsyncWrapsResourceNotFoundException()
        {
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    throw new ResourceNotFoundException("Test error");
                });

            var store = CreateStore(client);
            var request = new ScanRDFTriplesRequest(TableName, null, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBResourceNotFoundException>(async () =>
            {
                await store.ScanRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task QueryAsyncWrapsInternalServerErrorException()
        {
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    throw new InternalServerErrorException("Test error");
                });

            var store = CreateStore(client);
            var request = new QueryRDFTriplesRequest(TableName, "subject", "predicate", null, true, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBInternalServerErrorException>(async () =>
            {
                await store.QueryRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task QueryAsyncWrapsProvisionedThroughputExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    throw new ProvisionedThroughputExceededException("Test error");
                });

            var store = CreateStore(client);
            var request = new QueryRDFTriplesRequest(TableName, "subject", "predicate", null, true, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBThroughputExceededException>(async () =>
            {
                await store.QueryRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task QueryAsyncWrapsRequestLimitExceededException()
        {
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    throw new RequestLimitExceededException("Test error");
                });

            var store = CreateStore(client);
            var request = new QueryRDFTriplesRequest(TableName, "subject", "predicate", null, true, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBRequestLimitExceededException>(async () =>
            {
                await store.QueryRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task QueryAsyncWrapsResourceNotFoundException()
        {
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    throw new ResourceNotFoundException("Test error");
                });

            var store = CreateStore(client);
            var request = new QueryRDFTriplesRequest(TableName, "subject", "predicate", null, true, 0, false, false);

            var exception = await Assert.ThrowsExceptionAsync<GraphlessDBResourceNotFoundException>(async () =>
            {
                await store.QueryRDFTriplesAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception.InnerException);
        }

        [TestMethod]
        public async Task TransactWriteWithUpdateOperationExecutesCorrectly()
        {
            var triple = CreateTriple(versionDetail: new VersionDetail(2, 1));
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Put);
                    Assert.IsTrue(request.TransactItems[0].Put.ConditionExpression.Contains("attribute_exists"));
                    return Task.FromResult(new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var updateRequest = WriteRDFTriple.Create(new UpdateRDFTriple(TableName, triple, new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [updateRequest]);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactWriteWithDeleteOperationExecutesCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Delete);
                    return Task.FromResult(new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [deleteRequest]);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactWriteWithIncrementAllEdgesVersionExecutesCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Update);
                    Assert.IsTrue(request.TransactItems[0].Update.UpdateExpression.Contains("AllEdgesVersion"));
                    return Task.FromResult(new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var incrementRequest = WriteRDFTriple.Create(new IncrementRDFTripleAllEdgesVersion(TableName, triple.AsKey(), new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [incrementRequest]);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactWriteWithUpdateAllEdgesVersionExecutesCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].Update);
                    Assert.IsTrue(request.TransactItems[0].Update.UpdateExpression.Contains("AllEdgesVersion"));
                    return Task.FromResult(new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var updateAllEdgesRequest = WriteRDFTriple.Create(new UpdateRDFTripleAllEdgesVersion(TableName, triple.AsKey(), new VersionDetailCondition(null, 1), 2));
            var request = new WriteRDFTriplesRequest("token", false, [updateAllEdgesRequest]);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactWriteWithUpdateAllEdgesVersionThrowsWhenAllEdgesVersionNotSet()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new InvalidOperationException("Should not be called");
                });

            var store = CreateStore(client);
            var updateAllEdgesRequest = WriteRDFTriple.Create(new UpdateRDFTripleAllEdgesVersion(TableName, triple.AsKey(), new VersionDetailCondition(null, null), 2));
            var request = new WriteRDFTriplesRequest("token", false, [updateAllEdgesRequest]);

            await Assert.ThrowsExceptionAsync<ArgumentException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task TransactWriteWithCheckRDFTripleVersionExecutesCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    Assert.IsNotNull(request.TransactItems[0].ConditionCheck);
                    return Task.FromResult(new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var checkRequest = WriteRDFTriple.Create(new CheckRDFTripleVersion(TableName, triple.AsKey(), new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [checkRequest]);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactWriteWithUnsupportedOperationThrows()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    throw new InvalidOperationException("Should not be called");
                });

            var store = CreateStore(client);
            var unsupportedRequest = new WriteRDFTriple(null, null, null, null, null, null);
            var request = new WriteRDFTriplesRequest("token", false, [unsupportedRequest]);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task BatchWriteWithNonDeleteOperationThrows()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    throw new InvalidOperationException("Should not be called");
                });

            var store = CreateStore(client);
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", true, [addRequest]);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncFiltersTransientItems()
        {
            var transientKey = "_TxT";
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            {
                                TableName,
                                new List<Dictionary<string, AttributeValue>>
                                {
                                    new Dictionary<string, AttributeValue>
                                    {
                                        { "Subject", AttributeValueFactory.CreateS("test-subject") },
                                        { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                                        { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                                        { "Object", AttributeValueFactory.CreateS("object") },
                                        { "Partition", AttributeValueFactory.CreateS("partition") },
                                        { transientKey, AttributeValueFactory.CreateS("true") }
                                    }
                                }
                            }
                        },
                        ConsumedCapacity = new List<ConsumedCapacity>(),
                        UnprocessedKeys = new Dictionary<string, KeysAndAttributes>()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var key = new RDFTripleKey("test-subject", "test-predicate");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsNull(response.Items[0]);
        }

        [TestMethod]
        public async Task ScanAsyncFiltersTransientItems()
        {
            var transientKey = "_TxT";
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    var response = new ScanResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS("subject1") },
                                { "Predicate", AttributeValueFactory.CreateS("predicate1") },
                                { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                                { "Object", AttributeValueFactory.CreateS("object") },
                                { "Partition", AttributeValueFactory.CreateS("partition") }
                            },
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS("subject2") },
                                { "Predicate", AttributeValueFactory.CreateS("predicate2") },
                                { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                                { "Object", AttributeValueFactory.CreateS("object") },
                                { "Partition", AttributeValueFactory.CreateS("partition") },
                                { transientKey, AttributeValueFactory.CreateS("true") }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var request = new ScanRDFTriplesRequest(TableName, null, 0, false, false);

            var response = await store.ScanRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.AreEqual("subject1", response.Items[0].Subject);
        }

        [TestMethod]
        public async Task QueryAsyncFiltersTransientItems()
        {
            var transientKey = "_TxT";
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    var response = new QueryResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS("subject1") },
                                { "Predicate", AttributeValueFactory.CreateS("predicate1") },
                                { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                                { "Object", AttributeValueFactory.CreateS("object") },
                                { "Partition", AttributeValueFactory.CreateS("partition") }
                            },
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS("subject2") },
                                { "Predicate", AttributeValueFactory.CreateS("predicate2") },
                                { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                                { "Object", AttributeValueFactory.CreateS("object") },
                                { "Partition", AttributeValueFactory.CreateS("partition") },
                                { transientKey, AttributeValueFactory.CreateS("true") }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var request = new QueryRDFTriplesRequest(TableName, "subject", "predicate", null, true, 0, false, false);

            var response = await store.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.AreEqual("subject1", response.Items[0].Subject);
        }

        [TestMethod]
        public async Task WriteRDFTriplesBatchesLargeRequestsCorrectly()
        {
            var batchCount = 0;
            var triples = Enumerable.Range(0, 30).Select(i =>
                WriteRDFTriple.Create(new DeleteRDFTriple(TableName, new RDFTripleKey($"subject-{i}", "predicate"), VersionDetailCondition.None)))
                .ToImmutableList();

            var client = new MockAmazonDynamoDBClient(
                batchWriteItemHandler: (request, ct) =>
                {
                    batchCount++;
                    Assert.IsTrue(request.RequestItems[TableName].Count <= 25);
                    return Task.FromResult(new BatchWriteItemResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var request = new WriteRDFTriplesRequest("token", true, triples);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, batchCount);
        }

        [TestMethod]
        public async Task TransactionCanceledExceptionWithMultipleCancellationReasons()
        {
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    var exception = new TransactionCanceledException("Transaction cancelled");
                    exception.CancellationReasons = new List<CancellationReason>
                    {
                        new CancellationReason { Code = "None" },
                        new CancellationReason { Code = "ConditionalCheckFailed" },
                        new CancellationReason { Code = "ItemCollectionSizeLimitExceeded" }
                    };
                    throw exception;
                });

            var store = CreateStore(client);
            var triple = CreateTriple();
            var addRequest = WriteRDFTriple.Create(new AddRDFTriple(TableName, triple));
            var request = new WriteRDFTriplesRequest("token", false, [addRequest, addRequest, addRequest]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncReturnsNullForMissingItemsInCorrectOrder()
        {
            var client = new MockAmazonDynamoDBClient(
                batchGetItemHandler: (request, ct) =>
                {
                    var response = new BatchGetItemResponse
                    {
                        Responses = new Dictionary<string, List<Dictionary<string, AttributeValue>>>
                        {
                            {
                                TableName,
                                new List<Dictionary<string, AttributeValue>>
                                {
                                    new Dictionary<string, AttributeValue>
                                    {
                                        { "Subject", AttributeValueFactory.CreateS("subject2") },
                                        { "Predicate", AttributeValueFactory.CreateS("predicate") },
                                        { "IndexedObject", AttributeValueFactory.CreateS("indexed") },
                                        { "Object", AttributeValueFactory.CreateS("object") },
                                        { "Partition", AttributeValueFactory.CreateS("partition") }
                                    }
                                }
                            }
                        },
                        ConsumedCapacity = new List<ConsumedCapacity>(),
                        UnprocessedKeys = new Dictionary<string, KeysAndAttributes>()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var keys = ImmutableList.Create(
                new RDFTripleKey("subject1", "predicate"),
                new RDFTripleKey("subject2", "predicate"),
                new RDFTripleKey("subject3", "predicate"));
            var request = new GetRDFTriplesRequest(TableName, keys, false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(3, response.Items.Count);
            Assert.IsNull(response.Items[0]);
            Assert.IsNotNull(response.Items[1]);
            Assert.AreEqual("subject2", response.Items[1]!.Subject);
            Assert.IsNull(response.Items[2]);
        }

        [TestMethod]
        public async Task TransactWriteWithVersionDetailConditionsBuildsCorrectExpression()
        {
            var triple = CreateTriple(versionDetail: new VersionDetail(2, 3));
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    Assert.AreEqual(1, request.TransactItems.Count);
                    var item = request.TransactItems[0];
                    Assert.IsNotNull(item.Put);
                    Assert.IsTrue(item.Put.ConditionExpression.Contains("NodeVersion"));
                    Assert.IsTrue(item.Put.ConditionExpression.Contains("AllEdgesVersion"));
                    Assert.IsTrue(item.Put.ExpressionAttributeNames.ContainsKey("#VersionDetail"));
                    Assert.IsTrue(item.Put.ExpressionAttributeValues.ContainsKey(":NodeVersion"));
                    Assert.IsTrue(item.Put.ExpressionAttributeValues.ContainsKey(":AllEdgesVersion"));
                    return Task.FromResult(new TransactWriteItemsResponse
                    {
                        ConsumedCapacity = new List<ConsumedCapacity>()
                    });
                });

            var store = CreateStore(client);
            var updateRequest = WriteRDFTriple.Create(new UpdateRDFTriple(TableName, triple, new VersionDetailCondition(1, 2)));
            var request = new WriteRDFTriplesRequest("token", false, [updateRequest]);

            await store.WriteRDFTriplesAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactionCanceledWithDeleteOperationLogsCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    var exception = new TransactionCanceledException("Transaction cancelled");
                    exception.CancellationReasons = new List<CancellationReason>
                    {
                        new CancellationReason { Code = "ConditionalCheckFailed" }
                    };
                    throw exception;
                });

            var store = CreateStore(client);
            var deleteRequest = WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [deleteRequest]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task TransactionCanceledWithUpdateOperationLogsCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    var exception = new TransactionCanceledException("Transaction cancelled");
                    exception.CancellationReasons = new List<CancellationReason>
                    {
                        new CancellationReason { Code = "ConditionalCheckFailed" }
                    };
                    throw exception;
                });

            var store = CreateStore(client);
            var incrementRequest = WriteRDFTriple.Create(new IncrementRDFTripleAllEdgesVersion(TableName, triple.AsKey(), new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [incrementRequest]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task TransactionCanceledWithConditionCheckOperationLogsCorrectly()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                transactWriteItemsHandler: (request, ct) =>
                {
                    var exception = new TransactionCanceledException("Transaction cancelled");
                    exception.CancellationReasons = new List<CancellationReason>
                    {
                        new CancellationReason { Code = "ConditionalCheckFailed" }
                    };
                    throw exception;
                });

            var store = CreateStore(client);
            var checkRequest = WriteRDFTriple.Create(new CheckRDFTripleVersion(TableName, triple.AsKey(), new VersionDetailCondition(1, 0)));
            var request = new WriteRDFTriplesRequest("token", false, [checkRequest]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncWithExclusiveStartKey()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    Assert.IsNotNull(request.ExclusiveStartKey);
                    Assert.IsTrue(request.IndexName.Contains("ByPredicate"));
                    var response = new QueryResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var startKey = new RDFTripleKeyWithPartition("start-subject", "start-predicate", "partition");
            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(TableName, "test-partition", "test-predicate", startKey, true, 10, false, false);

            var response = await store.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncWithExclusiveStartKey()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                queryHandler: (request, ct) =>
                {
                    Assert.IsNotNull(request.ExclusiveStartKey);
                    var response = new QueryResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var startKey = new RDFTripleKey("start-subject", "start-predicate");
            var request = new QueryRDFTriplesRequest(TableName, "test-subject", "test-predicate", startKey, true, 10, false, false);

            var response = await store.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncWithExclusiveStartKey()
        {
            var triple = CreateTriple();
            var client = new MockAmazonDynamoDBClient(
                scanHandler: (request, ct) =>
                {
                    Assert.IsNotNull(request.ExclusiveStartKey);
                    var response = new ScanResponse
                    {
                        Items = new List<Dictionary<string, AttributeValue>>
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                { "Subject", AttributeValueFactory.CreateS(triple.Subject) },
                                { "Predicate", AttributeValueFactory.CreateS(triple.Predicate) },
                                { "IndexedObject", AttributeValueFactory.CreateS(triple.IndexedObject) },
                                { "Object", AttributeValueFactory.CreateS(triple.Object) },
                                { "Partition", AttributeValueFactory.CreateS(triple.Partition) }
                            }
                        },
                        LastEvaluatedKey = new Dictionary<string, AttributeValue>(),
                        ConsumedCapacity = new ConsumedCapacity()
                    };
                    return Task.FromResult(response);
                });

            var store = CreateStore(client);
            var startKey = new RDFTripleKey("start-subject", "start-predicate");
            var request = new ScanRDFTriplesRequest(TableName, startKey, 10, false, false);

            var response = await store.ScanRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
        }
    }
}
