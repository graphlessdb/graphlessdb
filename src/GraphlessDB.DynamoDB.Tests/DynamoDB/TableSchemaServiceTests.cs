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
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Tests
{
    [TestClass]
    public sealed class TableSchemaServiceTests
    {
        private sealed class MockAmazonDynamoDB : IAmazonDynamoDB
        {
            public Func<DescribeTableRequest, CancellationToken, Task<DescribeTableResponse>> DescribeTableAsyncFunc { get; set; } =
                (request, ct) => Task.FromResult(new DescribeTableResponse
                {
                    Table = new TableDescription
                    {
                        TableName = request.TableName,
                        KeySchema = new List<KeySchemaElement>
                        {
                            new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" }
                        }
                    }
                });

            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default)
            {
                return DescribeTableAsyncFunc(request, cancellationToken);
            }

            #region Not Implemented IAmazonDynamoDB Members
            public IDynamoDBv2PaginatorFactory Paginators => throw new NotImplementedException();
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
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public void Dispose() { }
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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
            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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
            #endregion
        }

        private static TableSchemaService CreateService(MockAmazonDynamoDB? mockClient = null)
        {
            return new TableSchemaService(mockClient ?? new MockAmazonDynamoDB());
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncReturnsSchemaFromDynamoDB()
        {
            var expectedSchema = new List<KeySchemaElement>
            {
                new KeySchemaElement { AttributeName = "PK", KeyType = "HASH" },
                new KeySchemaElement { AttributeName = "SK", KeyType = "RANGE" }
            };

            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) => Task.FromResult(new DescribeTableResponse
                {
                    Table = new TableDescription
                    {
                        TableName = request.TableName,
                        KeySchema = expectedSchema
                    }
                })
            };

            var service = CreateService(mockClient);
            var result = await service.GetTableSchemaAsync("TestTable", CancellationToken.None);

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("PK", result[0].AttributeName);
            Assert.AreEqual(KeyType.HASH, result[0].KeyType);
            Assert.AreEqual("SK", result[1].AttributeName);
            Assert.AreEqual(KeyType.RANGE, result[1].KeyType);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncCachesSchemaOnFirstCall()
        {
            int callCount = 0;
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) =>
                {
                    callCount++;
                    return Task.FromResult(new DescribeTableResponse
                    {
                        Table = new TableDescription
                        {
                            TableName = request.TableName,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" }
                            }
                        }
                    });
                }
            };

            var service = CreateService(mockClient);
            await service.GetTableSchemaAsync("TestTable", CancellationToken.None);
            await service.GetTableSchemaAsync("TestTable", CancellationToken.None);

            Assert.AreEqual(1, callCount);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncReturnsSameSchemaOnSubsequentCalls()
        {
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) => Task.FromResult(new DescribeTableResponse
                {
                    Table = new TableDescription
                    {
                        TableName = request.TableName,
                        KeySchema = new List<KeySchemaElement>
                        {
                            new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" }
                        }
                    }
                })
            };

            var service = CreateService(mockClient);
            var result1 = await service.GetTableSchemaAsync("TestTable", CancellationToken.None);
            var result2 = await service.GetTableSchemaAsync("TestTable", CancellationToken.None);

            Assert.AreSame(result1, result2);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncCachesDifferentTablesSeparately()
        {
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) => Task.FromResult(new DescribeTableResponse
                {
                    Table = new TableDescription
                    {
                        TableName = request.TableName,
                        KeySchema = new List<KeySchemaElement>
                        {
                            new KeySchemaElement { AttributeName = request.TableName, KeyType = "HASH" }
                        }
                    }
                })
            };

            var service = CreateService(mockClient);
            var result1 = await service.GetTableSchemaAsync("Table1", CancellationToken.None);
            var result2 = await service.GetTableSchemaAsync("Table2", CancellationToken.None);

            Assert.AreNotSame(result1, result2);
            Assert.AreEqual("Table1", result1[0].AttributeName);
            Assert.AreEqual("Table2", result2[0].AttributeName);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncPassesTableNameToClient()
        {
            string? capturedTableName = null;
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) =>
                {
                    capturedTableName = request.TableName;
                    return Task.FromResult(new DescribeTableResponse
                    {
                        Table = new TableDescription
                        {
                            TableName = request.TableName,
                            KeySchema = new List<KeySchemaElement>()
                        }
                    });
                }
            };

            var service = CreateService(mockClient);
            await service.GetTableSchemaAsync("MyCustomTable", CancellationToken.None);

            Assert.AreEqual("MyCustomTable", capturedTableName);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncPassesCancellationToken()
        {
            CancellationToken capturedToken = default;
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) =>
                {
                    capturedToken = ct;
                    return Task.FromResult(new DescribeTableResponse
                    {
                        Table = new TableDescription
                        {
                            TableName = request.TableName,
                            KeySchema = new List<KeySchemaElement>()
                        }
                    });
                }
            };

            var service = CreateService(mockClient);
            using var cts = new CancellationTokenSource();
            await service.GetTableSchemaAsync("TestTable", cts.Token);

            Assert.AreEqual(cts.Token, capturedToken);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncHandlesEmptyKeySchema()
        {
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (request, ct) => Task.FromResult(new DescribeTableResponse
                {
                    Table = new TableDescription
                    {
                        TableName = request.TableName,
                        KeySchema = new List<KeySchemaElement>()
                    }
                })
            };

            var service = CreateService(mockClient);
            var result = await service.GetTableSchemaAsync("TestTable", CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncHandlesConcurrentRequestsForSameTable()
        {
            int callCount = 0;
            var tcs = new TaskCompletionSource<bool>();
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = async (request, ct) =>
                {
                    callCount++;
                    await tcs.Task;
                    return new DescribeTableResponse
                    {
                        Table = new TableDescription
                        {
                            TableName = request.TableName,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" }
                            }
                        }
                    };
                }
            };

            var service = CreateService(mockClient);
            var task1 = service.GetTableSchemaAsync("TestTable", CancellationToken.None);
            var task2 = service.GetTableSchemaAsync("TestTable", CancellationToken.None);

            tcs.SetResult(true);
            var result1 = await task1;
            var result2 = await task2;

            Assert.AreEqual(1, callCount);
            Assert.AreSame(result1, result2);
        }

        [TestMethod]
        public async Task GetTableSchemaAsyncRespectsCancellationToken()
        {
            var mockClient = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = async (request, ct) =>
                {
                    await Task.Delay(1000, ct);
                    return new DescribeTableResponse
                    {
                        Table = new TableDescription
                        {
                            TableName = request.TableName,
                            KeySchema = new List<KeySchemaElement>()
                        }
                    };
                }
            };

            var service = CreateService(mockClient);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsExceptionAsync<TaskCanceledException>(async () =>
            {
                await service.GetTableSchemaAsync("TestTable", cts.Token);
            });
        }

        [TestMethod]
        public void DisposeCanBeCalledMultipleTimes()
        {
            var service = CreateService();
            service.Dispose();
            service.Dispose();
        }

        [TestMethod]
        public void DisposeCanBeCalledWithoutCallingGetTableSchemaAsync()
        {
            var service = CreateService();
            service.Dispose();
        }
    }
}
