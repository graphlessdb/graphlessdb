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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class DefaultItemImageStoreTests
    {
        private const string TableName = "ItemImageTable";

        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            private readonly Func<GetItemRequest, CancellationToken, Task<GetItemResponse>>? _getItemAsync;
            private readonly Func<PutItemRequest, CancellationToken, Task<PutItemResponse>>? _putItemAsync;
            private readonly Func<DeleteItemRequest, CancellationToken, Task<DeleteItemResponse>>? _deleteItemAsync;

            public MockAmazonDynamoDB(
                Func<GetItemRequest, CancellationToken, Task<GetItemResponse>>? getItemAsync = null,
                Func<PutItemRequest, CancellationToken, Task<PutItemResponse>>? putItemAsync = null,
                Func<DeleteItemRequest, CancellationToken, Task<DeleteItemResponse>>? deleteItemAsync = null)
            {
                _getItemAsync = getItemAsync;
                _putItemAsync = putItemAsync;
                _deleteItemAsync = deleteItemAsync;
            }

            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default)
            {
                return _getItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new GetItemResponse());
            }

            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
            {
                return _putItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new PutItemResponse());
            }

            public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default)
            {
                return _deleteItemAsync?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new DeleteItemResponse());
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
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
            public void Dispose() { }
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

        private static DefaultItemImageStore CreateDefaultItemImageStore(
            ItemImageStoreOptions? options = null,
            ITransactionServiceEvents? transactionServiceEvents = null,
            IAmazonDynamoDB? amazonDynamoDB = null)
        {
            var optionsSnapshot = new MockOptionsSnapshot<ItemImageStoreOptions>(
                options ?? new ItemImageStoreOptions { ItemImageTableName = TableName });

            return new DefaultItemImageStore(
                optionsSnapshot,
                transactionServiceEvents ?? new MockTransactionServiceEvents(),
                amazonDynamoDB ?? new MockAmazonDynamoDB());
        }

        private static TransactionVersion CreateTransactionVersion(string id = "tx-123", int version = 1)
        {
            return new TransactionVersion(id, version);
        }

        private static ItemRecord CreateItemRecord(string tableName = "TestTable", string keyValue = "key1")
        {
            var key = ItemKey.Create(tableName, new Dictionary<string, AttributeValue>
            {
                { "Id", AttributeValueFactory.CreateS(keyValue) }
            }.ToImmutableDictionary());

            return new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
        }

        [TestMethod]
        public void GetKeyReturnsCorrectKey()
        {
            var store = CreateDefaultItemImageStore();
            var transactionVersion = CreateTransactionVersion("tx-123", 5);

            var result = store.GetKey(transactionVersion);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Count);
            Assert.IsTrue(result.ContainsKey(ImageAttributeName.ImageId.Value));
            Assert.AreEqual("tx-123_5", result[ImageAttributeName.ImageId.Value].S);
        }

        [TestMethod]
        public void GetTransactionVersionsThrowsNotSupportedException()
        {
            var store = CreateDefaultItemImageStore();
            var id = new TransactionId("tx-123");

            Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await store.GetTransactionVersions(id, CancellationToken.None);
            });
        }

        [TestMethod]
        public void BackupItemImagesAsyncThrowsNotSupportedException()
        {
            var store = CreateDefaultItemImageStore();
            var id = new TransactionId("tx-123");
            var request = new GetItemRequest();

            Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await store.BackupItemImagesAsync(id, request, 1, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetItemImagesAsyncThrowsWhenVersionIsZero()
        {
            var store = CreateDefaultItemImageStore();
            var transactionVersion = CreateTransactionVersion("tx-123", 0);

            await Assert.ThrowsExceptionAsync<TransactionAssertionException>(async () =>
            {
                await store.GetItemImagesAsync(transactionVersion, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetItemImagesAsyncReturnsEmptyListWhenItemNotFound()
        {
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) =>
                {
                    return Task.FromResult(new GetItemResponse
                    {
                        IsItemSet = false
                    });
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            var result = await store.GetItemImagesAsync(transactionVersion, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncUsesConsistentRead()
        {
            GetItemRequest? capturedRequest = null;
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) =>
                {
                    capturedRequest = request;
                    return Task.FromResult(new GetItemResponse
                    {
                        IsItemSet = false
                    });
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            await store.GetItemImagesAsync(transactionVersion, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.IsTrue(capturedRequest.ConsistentRead);
            Assert.AreEqual(TableName, capturedRequest.TableName);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncReturnsDeserializedItems()
        {
            var items = ImmutableList.Create(CreateItemRecord("Table1", "key1"), CreateItemRecord("Table2", "key2"));
            var json = JsonSerializer.Serialize(items, ItemImageStoreSerializerContext.Default.ImmutableListItemRecord);

            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) =>
                {
                    return Task.FromResult(new GetItemResponse
                    {
                        IsItemSet = true,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { ImageAttributeName.ImageId.Value, AttributeValueFactory.CreateS("tx-123_1") },
                            { ImageAttributeName.ImageValue.Value, AttributeValueFactory.CreateS(json) }
                        }
                    });
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            var result = await store.GetItemImagesAsync(transactionVersion, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncThrowsWhenDeserializationFails()
        {
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                getItemAsync: (request, ct) =>
                {
                    return Task.FromResult(new GetItemResponse
                    {
                        IsItemSet = true,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { ImageAttributeName.ImageId.Value, AttributeValueFactory.CreateS("tx-123_1") },
                            { ImageAttributeName.ImageValue.Value, AttributeValueFactory.CreateS("null") }
                        }
                    });
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await store.GetItemImagesAsync(transactionVersion, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task AddItemImagesAsyncReturnsEarlyWhenItemsIsEmpty()
        {
            var putCalled = false;
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                putItemAsync: (request, ct) =>
                {
                    putCalled = true;
                    return Task.FromResult(new PutItemResponse());
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            await store.AddItemImagesAsync(transactionVersion, ImmutableList<ItemRecord>.Empty, CancellationToken.None);

            Assert.IsFalse(putCalled);
        }

        [TestMethod]
        public async Task AddItemImagesAsyncCallsOnBackupItemImagesAsync()
        {
            var eventCalled = false;
            TransactionId? capturedId = null;

            var mockEvents = new MockTransactionServiceEvents
            {
                OnBackupItemImagesAsync = (id, ct) =>
                {
                    eventCalled = true;
                    capturedId = id;
                    return Task.CompletedTask;
                }
            };

            var store = CreateDefaultItemImageStore(transactionServiceEvents: mockEvents);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);
            var items = ImmutableList.Create(CreateItemRecord());

            await store.AddItemImagesAsync(transactionVersion, items, CancellationToken.None);

            Assert.IsTrue(eventCalled);
            Assert.IsNotNull(capturedId);
            Assert.AreEqual("tx-123", capturedId.Id);
        }

        [TestMethod]
        public async Task AddItemImagesAsyncDoesNotCallEventWhenNull()
        {
            var mockEvents = new MockTransactionServiceEvents
            {
                OnBackupItemImagesAsync = null
            };

            var store = CreateDefaultItemImageStore(transactionServiceEvents: mockEvents);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);
            var items = ImmutableList.Create(CreateItemRecord());

            await store.AddItemImagesAsync(transactionVersion, items, CancellationToken.None);
        }

        [TestMethod]
        public async Task AddItemImagesAsyncSerializesAndStoresItems()
        {
            PutItemRequest? capturedRequest = null;
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                putItemAsync: (request, ct) =>
                {
                    capturedRequest = request;
                    return Task.FromResult(new PutItemResponse());
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);
            var items = ImmutableList.Create(CreateItemRecord());

            await store.AddItemImagesAsync(transactionVersion, items, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(TableName, capturedRequest.TableName);
            Assert.AreEqual(2, capturedRequest.Item.Count);
            Assert.IsTrue(capturedRequest.Item.ContainsKey(ImageAttributeName.ImageId.Value));
            Assert.IsTrue(capturedRequest.Item.ContainsKey(ImageAttributeName.ImageValue.Value));
            Assert.AreEqual("tx-123_1", capturedRequest.Item[ImageAttributeName.ImageId.Value].S);
        }

        [TestMethod]
        public async Task AddItemImagesAsyncUsesConditionExpression()
        {
            PutItemRequest? capturedRequest = null;
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                putItemAsync: (request, ct) =>
                {
                    capturedRequest = request;
                    return Task.FromResult(new PutItemResponse());
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);
            var items = ImmutableList.Create(CreateItemRecord());

            await store.AddItemImagesAsync(transactionVersion, items, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual($"attribute_not_exists(#{ImageAttributeName.ImageId.Value})", capturedRequest.ConditionExpression);
            Assert.IsNotNull(capturedRequest.ExpressionAttributeNames);
            Assert.AreEqual(1, capturedRequest.ExpressionAttributeNames.Count);
            Assert.IsTrue(capturedRequest.ExpressionAttributeNames.ContainsKey($"#{ImageAttributeName.ImageId.Value}"));
            Assert.AreEqual(ImageAttributeName.ImageId.Value, capturedRequest.ExpressionAttributeNames[$"#{ImageAttributeName.ImageId.Value}"]);
        }

        [TestMethod]
        public async Task DeleteItemImagesAsyncCallsDeleteWithCorrectKey()
        {
            DeleteItemRequest? capturedRequest = null;
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                deleteItemAsync: (request, ct) =>
                {
                    capturedRequest = request;
                    return Task.FromResult(new DeleteItemResponse());
                });

            var store = CreateDefaultItemImageStore(amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            await store.DeleteItemImagesAsync(transactionVersion, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(TableName, capturedRequest.TableName);
            Assert.AreEqual(1, capturedRequest.Key.Count);
            Assert.IsTrue(capturedRequest.Key.ContainsKey(ImageAttributeName.ImageId.Value));
            Assert.AreEqual("tx-123_1", capturedRequest.Key[ImageAttributeName.ImageId.Value].S);
        }

        [TestMethod]
        public async Task DeleteItemImagesAsyncUsesCorrectTableName()
        {
            const string customTableName = "CustomImageTable";
            DeleteItemRequest? capturedRequest = null;
            var mockAmazonDynamoDB = new MockAmazonDynamoDB(
                deleteItemAsync: (request, ct) =>
                {
                    capturedRequest = request;
                    return Task.FromResult(new DeleteItemResponse());
                });

            var options = new ItemImageStoreOptions { ItemImageTableName = customTableName };
            var store = CreateDefaultItemImageStore(options: options, amazonDynamoDB: mockAmazonDynamoDB);
            var transactionVersion = CreateTransactionVersion("tx-123", 1);

            await store.DeleteItemImagesAsync(transactionVersion, CancellationToken.None);

            Assert.IsNotNull(capturedRequest);
            Assert.AreEqual(customTableName, capturedRequest.TableName);
        }
    }
}
