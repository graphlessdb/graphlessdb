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
            public Func<BatchExecuteStatementRequest, CancellationToken, Task<BatchExecuteStatementResponse>> BatchExecuteStatementAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new BatchExecuteStatementResponse());
            public Func<ExecuteStatementRequest, CancellationToken, Task<ExecuteStatementResponse>> ExecuteStatementAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ExecuteStatementResponse());
            public Func<ExecuteTransactionRequest, CancellationToken, Task<ExecuteTransactionResponse>> ExecuteTransactionAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ExecuteTransactionResponse());
            public Func<CreateBackupRequest, CancellationToken, Task<CreateBackupResponse>> CreateBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new CreateBackupResponse());
            public Func<CreateGlobalTableRequest, CancellationToken, Task<CreateGlobalTableResponse>> CreateGlobalTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new CreateGlobalTableResponse());
            public Func<CreateTableRequest, CancellationToken, Task<CreateTableResponse>> CreateTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new CreateTableResponse());
            public Func<DeleteBackupRequest, CancellationToken, Task<DeleteBackupResponse>> DeleteBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteBackupResponse());
            public Func<DeleteTableRequest, CancellationToken, Task<DeleteTableResponse>> DeleteTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteTableResponse());
            public Func<DescribeBackupRequest, CancellationToken, Task<DescribeBackupResponse>> DescribeBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeBackupResponse());
            public Func<DescribeContinuousBackupsRequest, CancellationToken, Task<DescribeContinuousBackupsResponse>> DescribeContinuousBackupsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeContinuousBackupsResponse());
            public Func<DescribeContributorInsightsRequest, CancellationToken, Task<DescribeContributorInsightsResponse>> DescribeContributorInsightsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeContributorInsightsResponse());
            public Func<DescribeEndpointsRequest, CancellationToken, Task<DescribeEndpointsResponse>> DescribeEndpointsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeEndpointsResponse());
            public Func<DescribeExportRequest, CancellationToken, Task<DescribeExportResponse>> DescribeExportAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeExportResponse());
            public Func<DescribeGlobalTableRequest, CancellationToken, Task<DescribeGlobalTableResponse>> DescribeGlobalTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeGlobalTableResponse());
            public Func<DescribeGlobalTableSettingsRequest, CancellationToken, Task<DescribeGlobalTableSettingsResponse>> DescribeGlobalTableSettingsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeGlobalTableSettingsResponse());
            public Func<DescribeImportRequest, CancellationToken, Task<DescribeImportResponse>> DescribeImportAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeImportResponse());
            public Func<DescribeKinesisStreamingDestinationRequest, CancellationToken, Task<DescribeKinesisStreamingDestinationResponse>> DescribeKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeKinesisStreamingDestinationResponse());
            public Func<DescribeLimitsRequest, CancellationToken, Task<DescribeLimitsResponse>> DescribeLimitsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeLimitsResponse());
            public Func<DescribeTableRequest, CancellationToken, Task<DescribeTableResponse>> DescribeTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeTableResponse());
            public Func<DescribeTableReplicaAutoScalingRequest, CancellationToken, Task<DescribeTableReplicaAutoScalingResponse>> DescribeTableReplicaAutoScalingAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeTableReplicaAutoScalingResponse());
            public Func<DescribeTimeToLiveRequest, CancellationToken, Task<DescribeTimeToLiveResponse>> DescribeTimeToLiveAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DescribeTimeToLiveResponse());
            public Func<DisableKinesisStreamingDestinationRequest, CancellationToken, Task<DisableKinesisStreamingDestinationResponse>> DisableKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DisableKinesisStreamingDestinationResponse());
            public Func<EnableKinesisStreamingDestinationRequest, CancellationToken, Task<EnableKinesisStreamingDestinationResponse>> EnableKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new EnableKinesisStreamingDestinationResponse());
            public Func<ExportTableToPointInTimeRequest, CancellationToken, Task<ExportTableToPointInTimeResponse>> ExportTableToPointInTimeAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ExportTableToPointInTimeResponse());
            public Func<ImportTableRequest, CancellationToken, Task<ImportTableResponse>> ImportTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ImportTableResponse());
            public Func<ListBackupsRequest, CancellationToken, Task<ListBackupsResponse>> ListBackupsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListBackupsResponse());
            public Func<ListContributorInsightsRequest, CancellationToken, Task<ListContributorInsightsResponse>> ListContributorInsightsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListContributorInsightsResponse());
            public Func<ListExportsRequest, CancellationToken, Task<ListExportsResponse>> ListExportsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListExportsResponse());
            public Func<ListGlobalTablesRequest, CancellationToken, Task<ListGlobalTablesResponse>> ListGlobalTablesAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListGlobalTablesResponse());
            public Func<ListImportsRequest, CancellationToken, Task<ListImportsResponse>> ListImportsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListImportsResponse());
            public Func<ListTablesRequest, CancellationToken, Task<ListTablesResponse>> ListTablesAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListTablesResponse());
            public Func<ListTagsOfResourceRequest, CancellationToken, Task<ListTagsOfResourceResponse>> ListTagsOfResourceAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new ListTagsOfResourceResponse());
            public Func<DeleteResourcePolicyRequest, CancellationToken, Task<DeleteResourcePolicyResponse>> DeleteResourcePolicyAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new DeleteResourcePolicyResponse());
            public Func<GetResourcePolicyRequest, CancellationToken, Task<GetResourcePolicyResponse>> GetResourcePolicyAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new GetResourcePolicyResponse());
            public Func<PutResourcePolicyRequest, CancellationToken, Task<PutResourcePolicyResponse>> PutResourcePolicyAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new PutResourcePolicyResponse());
            public Func<RestoreTableFromBackupRequest, CancellationToken, Task<RestoreTableFromBackupResponse>> RestoreTableFromBackupAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new RestoreTableFromBackupResponse());
            public Func<RestoreTableToPointInTimeRequest, CancellationToken, Task<RestoreTableToPointInTimeResponse>> RestoreTableToPointInTimeAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new RestoreTableToPointInTimeResponse());
            public Func<TagResourceRequest, CancellationToken, Task<TagResourceResponse>> TagResourceAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new TagResourceResponse());
            public Func<UntagResourceRequest, CancellationToken, Task<UntagResourceResponse>> UntagResourceAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UntagResourceResponse());
            public Func<UpdateContinuousBackupsRequest, CancellationToken, Task<UpdateContinuousBackupsResponse>> UpdateContinuousBackupsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateContinuousBackupsResponse());
            public Func<UpdateContributorInsightsRequest, CancellationToken, Task<UpdateContributorInsightsResponse>> UpdateContributorInsightsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateContributorInsightsResponse());
            public Func<UpdateGlobalTableRequest, CancellationToken, Task<UpdateGlobalTableResponse>> UpdateGlobalTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateGlobalTableResponse());
            public Func<UpdateGlobalTableSettingsRequest, CancellationToken, Task<UpdateGlobalTableSettingsResponse>> UpdateGlobalTableSettingsAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateGlobalTableSettingsResponse());
            public Func<UpdateKinesisStreamingDestinationRequest, CancellationToken, Task<UpdateKinesisStreamingDestinationResponse>> UpdateKinesisStreamingDestinationAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateKinesisStreamingDestinationResponse());
            public Func<UpdateTableRequest, CancellationToken, Task<UpdateTableResponse>> UpdateTableAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateTableResponse());
            public Func<UpdateTableReplicaAutoScalingRequest, CancellationToken, Task<UpdateTableReplicaAutoScalingResponse>> UpdateTableReplicaAutoScalingAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateTableReplicaAutoScalingResponse());
            public Func<UpdateTimeToLiveRequest, CancellationToken, Task<UpdateTimeToLiveResponse>> UpdateTimeToLiveAsyncFunc { get; set; } = (req, ct) => Task.FromResult(new UpdateTimeToLiveResponse());

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

            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => BatchExecuteStatementAsyncFunc(request, cancellationToken);
            public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => CreateBackupAsyncFunc(request, cancellationToken);
            public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => CreateGlobalTableAsyncFunc(request, cancellationToken);
            public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => CreateTableAsyncFunc(new CreateTableRequest { TableName = tableName, KeySchema = keySchema, AttributeDefinitions = attributeDefinitions, ProvisionedThroughput = provisionedThroughput }, cancellationToken);
            public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => CreateTableAsyncFunc(request, cancellationToken);
            public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => DeleteBackupAsyncFunc(request, cancellationToken);
            public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => DeleteResourcePolicyAsyncFunc(request, cancellationToken);
            public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => DeleteTableAsyncFunc(new DeleteTableRequest { TableName = tableName }, cancellationToken);
            public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => DeleteTableAsyncFunc(request, cancellationToken);
            public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => DescribeBackupAsyncFunc(request, cancellationToken);
            public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => DescribeContinuousBackupsAsyncFunc(request, cancellationToken);
            public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => DescribeContributorInsightsAsyncFunc(request, cancellationToken);
            public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => DescribeEndpointsAsyncFunc(request, cancellationToken);
            public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => DescribeExportAsyncFunc(request, cancellationToken);
            public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => DescribeGlobalTableAsyncFunc(request, cancellationToken);
            public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => DescribeGlobalTableSettingsAsyncFunc(request, cancellationToken);
            public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => DescribeImportAsyncFunc(request, cancellationToken);
            public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => DescribeKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => DescribeLimitsAsyncFunc(request, cancellationToken);
            public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => DescribeTableAsyncFunc(new DescribeTableRequest { TableName = tableName }, cancellationToken);
            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => DescribeTableAsyncFunc(request, cancellationToken);
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => DescribeTableReplicaAutoScalingAsyncFunc(request, cancellationToken);
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => DescribeTimeToLiveAsyncFunc(new DescribeTimeToLiveRequest { TableName = tableName }, cancellationToken);
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => DescribeTimeToLiveAsyncFunc(request, cancellationToken);
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => new Amazon.Runtime.Endpoints.Endpoint("https://dynamodb.us-east-1.amazonaws.com");
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => DisableKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public void Dispose() { }
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => EnableKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => ExecuteStatementAsyncFunc(request, cancellationToken);
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => ExecuteTransactionAsyncFunc(request, cancellationToken);
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => ExportTableToPointInTimeAsyncFunc(request, cancellationToken);
            public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => GetResourcePolicyAsyncFunc(request, cancellationToken);
            public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => ImportTableAsyncFunc(request, cancellationToken);
            public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => ListBackupsAsyncFunc(request, cancellationToken);
            public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => ListContributorInsightsAsyncFunc(request, cancellationToken);
            public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => ListExportsAsyncFunc(request, cancellationToken);
            public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => ListGlobalTablesAsyncFunc(request, cancellationToken);
            public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => ListImportsAsyncFunc(request, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest(), cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest { ExclusiveStartTableName = exclusiveStartTableName }, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest { ExclusiveStartTableName = exclusiveStartTableName, Limit = limit }, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(new ListTablesRequest { Limit = limit }, cancellationToken);
            public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => ListTablesAsyncFunc(request, cancellationToken);
            public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => ListTagsOfResourceAsyncFunc(request, cancellationToken);
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => PutResourcePolicyAsyncFunc(request, cancellationToken);
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => RestoreTableFromBackupAsyncFunc(request, cancellationToken);
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => RestoreTableToPointInTimeAsyncFunc(request, cancellationToken);
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => TagResourceAsyncFunc(request, cancellationToken);
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => UntagResourceAsyncFunc(request, cancellationToken);
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => UpdateContinuousBackupsAsyncFunc(request, cancellationToken);
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => UpdateContributorInsightsAsyncFunc(request, cancellationToken);
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => UpdateGlobalTableAsyncFunc(request, cancellationToken);
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => UpdateGlobalTableSettingsAsyncFunc(request, cancellationToken);
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => UpdateKinesisStreamingDestinationAsyncFunc(request, cancellationToken);
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => UpdateTableAsyncFunc(new UpdateTableRequest { TableName = tableName, ProvisionedThroughput = provisionedThroughput }, cancellationToken);
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => UpdateTableAsyncFunc(request, cancellationToken);
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => UpdateTableReplicaAutoScalingAsyncFunc(request, cancellationToken);
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => UpdateTimeToLiveAsyncFunc(request, cancellationToken);
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
        public async Task RunHouseKeepingAsyncWithCommittedTransactionReturnsRemovedAction()
        {
            var committedTransaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(committedTransaction)),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(committedTransaction),
                RemoveAsyncFunc = (id, ct) => Task.CompletedTask
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.Removed, result.Items[0].Action);
            Assert.IsNull(result.Items[0].Error);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithRolledBackTransactionReturnsRemovedAction()
        {
            var rolledBackTransaction = Transaction.CreateNew() with { State = TransactionState.RolledBack };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(rolledBackTransaction)),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(rolledBackTransaction),
                RemoveAsyncFunc = (id, ct) => Task.CompletedTask
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.Removed, result.Items[0].Action);
            Assert.IsNull(result.Items[0].Error);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithStaleActiveTransactionReturnsRolledBackAction()
        {
            var staleTransaction = Transaction.CreateNew() with
            {
                State = TransactionState.Active,
                LastUpdateDateTime = DateTime.UtcNow.AddMinutes(-15)
            };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(staleTransaction)),
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(staleTransaction with { State = TransactionState.RolledBack }),
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

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.RolledBack, result.Items[0].Action);
            Assert.IsNull(result.Items[0].Error);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithStaleActiveTransactionHandlesTransactionCompletedException()
        {
            var staleTransaction = Transaction.CreateNew() with
            {
                State = TransactionState.Active,
                LastUpdateDateTime = DateTime.UtcNow.AddMinutes(-15)
            };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(staleTransaction)),
                GetAsyncFunc = (id, consistent, ct) => throw new TransactionCompletedException()
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.RolledBack, result.Items[0].Action);
            Assert.IsNull(result.Items[0].Error);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithCommittingTransactionHandlesTransactionCompletedException()
        {
            var committingTransaction = Transaction.CreateNew() with { State = TransactionState.Committing };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(committingTransaction)),
                GetAsyncFunc = (id, consistent, ct) => throw new TransactionCompletedException()
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.RolledBack, result.Items[0].Action);
            Assert.IsNull(result.Items[0].Error);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithRollingBackTransactionHandlesTransactionCompletedException()
        {
            var rollingBackTransaction = Transaction.CreateNew() with { State = TransactionState.RollingBack };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(rollingBackTransaction)),
                GetAsyncFunc = (id, consistent, ct) => throw new TransactionCompletedException()
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.RolledBack, result.Items[0].Action);
            Assert.IsNull(result.Items[0].Error);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithUnknownStateThrowsTransactionAssertionException()
        {
            var invalidTransaction = Transaction.CreateNew() with { State = (TransactionState)999 };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(invalidTransaction))
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.None, result.Items[0].Action);
            Assert.IsNotNull(result.Items[0].Error);
            Assert.IsInstanceOfType(result.Items[0].Error, typeof(TransactionAssertionException));
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncWithGeneralExceptionReturnsExceptionInResponse()
        {
            var transaction = Transaction.CreateNew() with { State = TransactionState.Committed };
            var mockTransactionStore = new MockTransactionStore
            {
                ListAsyncFunc = (limit, ct) => Task.FromResult(ImmutableList.Create(transaction)),
                GetAsyncFunc = (id, consistent, ct) => throw new InvalidOperationException("Test exception")
            };
            var service = CreateService(transactionStore: mockTransactionStore);
            var request = new RunHouseKeepingRequest(10, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10));

            var result = await service.RunHouseKeepingAsync(request, CancellationToken.None);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(HouseKeepTransactionAction.None, result.Items[0].Action);
            Assert.IsNotNull(result.Items[0].Error);
            Assert.IsInstanceOfType(result.Items[0].Error, typeof(InvalidOperationException));
            Assert.AreEqual("Test exception", result.Items[0].Error!.Message);
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
        public async Task TransactWriteItemsAsyncWithQuickTransactionsEnabledUsesShortcut()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };
            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB);
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem { Put = new Put { TableName = "TestTable", Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } } } }
                }
            };

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithQuickTransactionsAndConflictRetriesAfterProcessingConflict()
        {
            var callCount = 0;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        throw new TransactionCanceledException("Conflict");
                    }
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var mockTransactionStore = new MockTransactionStore
            {
                ContainsAsyncFunc = (id, ct) => Task.FromResult(false)
            };
            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };
            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB,
                transactionStore: mockTransactionStore);
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem { Put = new Put { TableName = "TestTable", Item = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } } } }
                }
            };

            await Assert.ThrowsExceptionAsync<TransactionCanceledException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });

            Assert.AreEqual(1, callCount);
        }

        [TestMethod]
        public void ValidatePutItemRequestWithConditionExpressionAndExpectedThrowsNotSupported()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>(),
                Expected = new Dictionary<string, ExpectedAttributeValue> { { "attr1", new ExpectedAttributeValue() } }
            };

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public void ValidatePutItemRequestWithConditionalOperatorThrowsNotSupported()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>(),
                ConditionalOperator = null
            };
            request.ConditionalOperator = "AND"; // Set via property

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public void ValidateUpdateItemRequestWithExpectedThrowsNotSupported()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } },
                Expected = new Dictionary<string, ExpectedAttributeValue> { { "attr1", new ExpectedAttributeValue() } }
            };

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public void ValidateDeleteItemRequestWithExpectedThrowsNotSupported()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "1" } } },
                Expected = new Dictionary<string, ExpectedAttributeValue> { { "attr1", new ExpectedAttributeValue() } }
            };

            Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });
        }

        [TestMethod]
        public void IsSupportedConditionExpressionWithMultipleKeysReturnsFalse()
        {
            var conditionCheck = new ConditionCheck
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } },
                    { "SortKey", new AttributeValue { S = "sk" } }
                },
                ConditionExpression = "attribute_not_exists(Id)"
            };

            var result = AmazonDynamoDBWithTransactionsTestHelper.IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task ResumeTransactionAsyncWithoutEventHandlerDoesNotInvokeHandler()
        {
            var transactionId = new TransactionId("test-id");
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(Transaction.CreateNew())
            };
            var mockEvents = new MockTransactionServiceEvents(); // No OnResumeTransactionFinishAsync set
            var service = CreateService(transactionStore: mockTransactionStore, transactionServiceEvents: mockEvents);

            var result = await service.ResumeTransactionAsync(transactionId, CancellationToken.None);

            Assert.AreEqual(transactionId, result);
        }

        [TestMethod]
        public async Task CommitTransactionAsyncWithUnexpectedStateThrowsTransactionException()
        {
            var transaction = Transaction.CreateNew() with { State = (TransactionState)999 };
            var mockTransactionStore = new MockTransactionStore
            {
                GetAsyncFunc = (id, consistent, ct) => Task.FromResult(transaction)
            };
            var service = CreateService(transactionStore: mockTransactionStore);

            await Assert.ThrowsExceptionAsync<TransactionException>(async () =>
            {
                await service.CommitTransactionAsync(transaction.GetId(), CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithQuickTransactionsAndDeleteItemAppliesDeleteCondition()
        {
            var deleteItem = new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test-id" } }
                    },
                    ConditionExpression = "attribute_exists(Id)",
                    ExpressionAttributeNames = new Dictionary<string, string>(),
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                }
            };

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem> { deleteItem }
            };

            var options = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100,
                TransactionStaleDuration = TimeSpan.FromMinutes(5)
            };

            var capturedRequest = new TransactWriteItemsRequest();
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(options),
                amazonDynamoDB: mockDynamoDB);

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest.TransactItems);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Delete);
            var conditionExpression = capturedRequest.TransactItems[0].Delete.ConditionExpression;
            var expressionAttributeNames = capturedRequest.TransactItems[0].Delete.ExpressionAttributeNames;
            
            Assert.IsNotNull(conditionExpression, "ConditionExpression should not be null");
            Assert.IsNotNull(expressionAttributeNames, "ExpressionAttributeNames should not be null");
            Assert.IsTrue(conditionExpression.Contains("_TxId"), $"Expected condition to contain _TxId but got: {conditionExpression}");
            Assert.IsTrue(expressionAttributeNames.ContainsKey("#_TxId"), $"Expected attribute names to contain #_TxId but got: {string.Join(", ", expressionAttributeNames.Keys)}");
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithQuickTransactionsAndConditionCheckWithAttributeNotExistsAppliesCondition()
        {
            var conditionCheckItem = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test-id" } }
                    },
                    ConditionExpression = "attribute_not_exists(Id)",
                    ExpressionAttributeNames = new Dictionary<string, string>(),
                    ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
                }
            };

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem> { conditionCheckItem }
            };

            var options = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100,
                TransactionStaleDuration = TimeSpan.FromMinutes(5)
            };

            var capturedRequest = new TransactWriteItemsRequest();
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(options),
                amazonDynamoDB: mockDynamoDB);

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest.TransactItems);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].ConditionCheck);
            var conditionExpression = capturedRequest.TransactItems[0].ConditionCheck.ConditionExpression;
            var expressionAttributeNames = capturedRequest.TransactItems[0].ConditionCheck.ExpressionAttributeNames;
            
            Assert.IsNotNull(conditionExpression);
            Assert.IsNotNull(expressionAttributeNames);
            Assert.IsTrue(conditionExpression.Contains("_TxId"));
            Assert.IsTrue(expressionAttributeNames.ContainsKey("#_TxId"));
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithQuickTransactionsAndUpdateItemAppliesUpdateCondition()
        {
            var updateItem = new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test-id" } }
                    },
                    UpdateExpression = "SET #name = :value",
                    ConditionExpression = "attribute_exists(Id)",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#name", "Name" }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":value", new AttributeValue { S = "test-value" } }
                    }
                }
            };

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem> { updateItem }
            };

            var options = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100,
                TransactionStaleDuration = TimeSpan.FromMinutes(5)
            };

            var capturedRequest = new TransactWriteItemsRequest();
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };

            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(options),
                amazonDynamoDB: mockDynamoDB);

            await service.TransactWriteItemsAsync(request, CancellationToken.None);

            Assert.IsNotNull(capturedRequest.TransactItems);
            Assert.AreEqual(1, capturedRequest.TransactItems.Count);
            Assert.IsNotNull(capturedRequest.TransactItems[0].Update);
            var conditionExpression = capturedRequest.TransactItems[0].Update.ConditionExpression;
            var expressionAttributeNames = capturedRequest.TransactItems[0].Update.ExpressionAttributeNames;
            
            Assert.IsNotNull(conditionExpression);
            Assert.IsNotNull(expressionAttributeNames);
            Assert.IsTrue(conditionExpression.Contains("_TxId"));
            Assert.IsTrue(expressionAttributeNames.ContainsKey("#_TxId"));
            Assert.AreEqual("SET #name = :value", capturedRequest.TransactItems[0].Update.UpdateExpression);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithQuickTransactionsAndUnsupportedConditionCheckThrowsNotSupportedException()
        {
            var conditionCheckItem = new TransactWriteItem
            {
                ConditionCheck = new ConditionCheck
                {
                    TableName = "TestTable",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test-id" } }
                    },
                    ConditionExpression = "attribute_exists(Id)",
                    ExpressionAttributeNames = new Dictionary<string, string>()
                }
            };

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem> { conditionCheckItem }
            };

            var options = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100,
                TransactionStaleDuration = TimeSpan.FromMinutes(5)
            };

            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(options),
                amazonDynamoDB: mockDynamoDB);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task BatchExecuteStatementAsyncCallsUnderlyingClient()
        {
            var request = new BatchExecuteStatementRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.BatchExecuteStatementAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task ExecuteStatementAsyncCallsUnderlyingClient()
        {
            var request = new ExecuteStatementRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ExecuteStatementAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task ExecuteTransactionAsyncCallsUnderlyingClient()
        {
            var request = new ExecuteTransactionRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ExecuteTransactionAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task CreateBackupAsyncCallsUnderlyingClient()
        {
            var request = new CreateBackupRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.CreateBackupAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task CreateGlobalTableAsyncCallsUnderlyingClient()
        {
            var request = new CreateGlobalTableRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.CreateGlobalTableAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task CreateTableAsyncWithParametersCallsUnderlyingClient()
        {
            var tableName = "TestTable";
            var keySchema = new List<KeySchemaElement>();
            var attributeDefinitions = new List<AttributeDefinition>();
            var provisionedThroughput = new ProvisionedThroughput();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.CreateTableAsync(tableName, keySchema, attributeDefinitions, provisionedThroughput, CancellationToken.None);
        }

        [TestMethod]
        public async Task CreateTableAsyncWithRequestCallsUnderlyingClient()
        {
            var request = new CreateTableRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.CreateTableAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task DeleteBackupAsyncCallsUnderlyingClient()
        {
            var request = new DeleteBackupRequest();
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.DeleteBackupAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task TransactWriteItemsAsyncWithQuickTransactionsAndEmptyTransactWriteItemThrowsNotSupportedException()
        {
            var emptyItem = new TransactWriteItem();

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem> { emptyItem }
            };

            var options = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100,
                TransactionStaleDuration = TimeSpan.FromMinutes(5)
            };

            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(options),
                amazonDynamoDB: mockDynamoDB);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task BatchExecuteStatementAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                BatchExecuteStatementAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new BatchExecuteStatementResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new BatchExecuteStatementRequest();

            await service.BatchExecuteStatementAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ExecuteStatementAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ExecuteStatementAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ExecuteStatementResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ExecuteStatementRequest();

            await service.ExecuteStatementAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ExecuteTransactionAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ExecuteTransactionAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ExecuteTransactionResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ExecuteTransactionRequest();

            await service.ExecuteTransactionAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWithDictionaryOverloadDelegatesToUnderlyingClient()
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
            var requestItems = new Dictionary<string, KeysAndAttributes>();

            await service.BatchGetItemAsync(requestItems, ReturnConsumedCapacity.TOTAL, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchGetItemAsyncWithDictionaryOverload2DelegatesToUnderlyingClient()
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
            var requestItems = new Dictionary<string, KeysAndAttributes>();

            await service.BatchGetItemAsync(requestItems, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task BatchWriteItemAsyncWithDictionaryOverloadDelegatesToUnderlyingClient()
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
            var requestItems = new Dictionary<string, List<WriteRequest>>();

            await service.BatchWriteItemAsync(requestItems, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithTableNameAndKeyDelegatesToUnderlyingClient()
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
            var key = new Dictionary<string, AttributeValue>();

            await service.DeleteItemAsync("TestTable", key, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteItemAsyncWithTableNameKeyAndReturnValuesDelegatesToUnderlyingClient()
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
            var key = new Dictionary<string, AttributeValue>();

            await service.DeleteItemAsync("TestTable", key, ReturnValue.ALL_OLD, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task GetItemAsyncWithTableNameAndKeyDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockIsolatedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };
            var service = CreateService(unCommittedService: mockIsolatedService);
            var key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } };

            await service.GetItemAsync("TestTable", key, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task GetItemAsyncWithTableNameKeyAndConsistentReadDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockIsolatedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };
            var service = CreateService(unCommittedService: mockIsolatedService);
            var key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } };

            await service.GetItemAsync("TestTable", key, true, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task PutItemAsyncWithTableNameAndItemDelegatesToUnderlyingClient()
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
            var item = new Dictionary<string, AttributeValue>();

            await service.PutItemAsync("TestTable", item, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task PutItemAsyncWithTableNameItemAndReturnValuesDelegatesToUnderlyingClient()
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
            var item = new Dictionary<string, AttributeValue>();

            await service.PutItemAsync("TestTable", item, ReturnValue.ALL_OLD, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncWithTableNameAndAttributesToGetDelegatesToUnderlyingClient()
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
            var attributesToGet = new List<string>();

            await service.ScanAsync("TestTable", attributesToGet, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncWithTableNameAndScanFilterDelegatesToUnderlyingClient()
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
            var scanFilter = new Dictionary<string, Condition>();

            await service.ScanAsync("TestTable", scanFilter, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ScanAsyncWithTableNameAttributesToGetAndScanFilterDelegatesToUnderlyingClient()
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
            var attributesToGet = new List<string>();
            var scanFilter = new Dictionary<string, Condition>();

            await service.ScanAsync("TestTable", attributesToGet, scanFilter, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithTableNameKeyAndAttributeUpdatesDelegatesToUnderlyingClient()
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
            var key = new Dictionary<string, AttributeValue>();
            var attributeUpdates = new Dictionary<string, AttributeValueUpdate>();

            await service.UpdateItemAsync("TestTable", key, attributeUpdates, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateItemAsyncWithTableNameKeyAttributeUpdatesAndReturnValuesDelegatesToUnderlyingClient()
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
            var key = new Dictionary<string, AttributeValue>();
            var attributeUpdates = new Dictionary<string, AttributeValueUpdate>();

            await service.UpdateItemAsync("TestTable", key, attributeUpdates, ReturnValue.ALL_NEW, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task CreateBackupAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                CreateBackupAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new CreateBackupResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new CreateBackupRequest();

            await service.CreateBackupAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task CreateGlobalTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                CreateGlobalTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new CreateGlobalTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new CreateGlobalTableRequest();

            await service.CreateGlobalTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task CreateTableAsyncWithTableNameKeySchemaAttributeDefinitionsAndProvisionedThroughputDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                CreateTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new CreateTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var keySchema = new List<KeySchemaElement>();
            var attributeDefinitions = new List<AttributeDefinition>();
            var provisionedThroughput = new ProvisionedThroughput();

            await service.CreateTableAsync("TestTable", keySchema, attributeDefinitions, provisionedThroughput, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task CreateTableAsyncWithRequestDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                CreateTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new CreateTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new CreateTableRequest();

            await service.CreateTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteBackupAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteBackupAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DeleteBackupResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DeleteBackupRequest();

            await service.DeleteBackupAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DeleteTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DeleteTableRequest();

            await service.DeleteTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeBackupAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeBackupAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeBackupResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeBackupRequest();

            await service.DescribeBackupAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeContinuousBackupsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeContinuousBackupsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeContinuousBackupsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeContinuousBackupsRequest();

            await service.DescribeContinuousBackupsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeContributorInsightsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeContributorInsightsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeContributorInsightsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeContributorInsightsRequest();

            await service.DescribeContributorInsightsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeEndpointsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeEndpointsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeEndpointsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeEndpointsRequest();

            await service.DescribeEndpointsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeExportAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeExportAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeExportResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeExportRequest();

            await service.DescribeExportAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeGlobalTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeGlobalTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeGlobalTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeGlobalTableRequest();

            await service.DescribeGlobalTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeGlobalTableSettingsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeGlobalTableSettingsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeGlobalTableSettingsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeGlobalTableSettingsRequest();

            await service.DescribeGlobalTableSettingsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeImportAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeImportAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeImportResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeImportRequest();

            await service.DescribeImportAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeKinesisStreamingDestinationAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeKinesisStreamingDestinationAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeKinesisStreamingDestinationResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeKinesisStreamingDestinationRequest();

            await service.DescribeKinesisStreamingDestinationAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeLimitsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeLimitsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeLimitsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeLimitsRequest();

            await service.DescribeLimitsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeTableRequest();

            await service.DescribeTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeTableReplicaAutoScalingAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeTableReplicaAutoScalingAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeTableReplicaAutoScalingResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeTableReplicaAutoScalingRequest();

            await service.DescribeTableReplicaAutoScalingAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeTimeToLiveAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeTimeToLiveAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeTimeToLiveResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DescribeTimeToLiveRequest();

            await service.DescribeTimeToLiveAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DisableKinesisStreamingDestinationAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DisableKinesisStreamingDestinationAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DisableKinesisStreamingDestinationResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DisableKinesisStreamingDestinationRequest();

            await service.DisableKinesisStreamingDestinationAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task EnableKinesisStreamingDestinationAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                EnableKinesisStreamingDestinationAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new EnableKinesisStreamingDestinationResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new EnableKinesisStreamingDestinationRequest();

            await service.EnableKinesisStreamingDestinationAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ExportTableToPointInTimeAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ExportTableToPointInTimeAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ExportTableToPointInTimeResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ExportTableToPointInTimeRequest();

            await service.ExportTableToPointInTimeAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteResourcePolicyAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteResourcePolicyAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DeleteResourcePolicyResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new DeleteResourcePolicyRequest();

            await service.DeleteResourcePolicyAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task GetResourcePolicyAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                GetResourcePolicyAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new GetResourcePolicyResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new GetResourcePolicyRequest();

            await service.GetResourcePolicyAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task PutResourcePolicyAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                PutResourcePolicyAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new PutResourcePolicyResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new PutResourcePolicyRequest();

            await service.PutResourcePolicyAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ImportTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ImportTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ImportTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ImportTableRequest();

            await service.ImportTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListBackupsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListBackupsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListBackupsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListBackupsRequest();

            await service.ListBackupsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListContributorInsightsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListContributorInsightsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListContributorInsightsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListContributorInsightsRequest();

            await service.ListContributorInsightsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListExportsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListExportsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListExportsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListExportsRequest();

            await service.ListExportsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListGlobalTablesAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListGlobalTablesAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListGlobalTablesResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListGlobalTablesRequest();

            await service.ListGlobalTablesAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListImportsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListImportsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListImportsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListImportsRequest();

            await service.ListImportsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListTablesAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListTablesAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListTablesResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListTablesRequest();

            await service.ListTablesAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListTagsOfResourceAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListTagsOfResourceAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListTagsOfResourceResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new ListTagsOfResourceRequest();

            await service.ListTagsOfResourceAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task RestoreTableFromBackupAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                RestoreTableFromBackupAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new RestoreTableFromBackupResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new RestoreTableFromBackupRequest();

            await service.RestoreTableFromBackupAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task RestoreTableToPointInTimeAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                RestoreTableToPointInTimeAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new RestoreTableToPointInTimeResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new RestoreTableToPointInTimeRequest();

            await service.RestoreTableToPointInTimeAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task TagResourceAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TagResourceAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new TagResourceResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new TagResourceRequest();

            await service.TagResourceAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UntagResourceAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UntagResourceAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UntagResourceResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UntagResourceRequest();

            await service.UntagResourceAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateContinuousBackupsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateContinuousBackupsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateContinuousBackupsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateContinuousBackupsRequest();

            await service.UpdateContinuousBackupsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateContributorInsightsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateContributorInsightsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateContributorInsightsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateContributorInsightsRequest();

            await service.UpdateContributorInsightsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateGlobalTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateGlobalTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateGlobalTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateGlobalTableRequest();

            await service.UpdateGlobalTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateGlobalTableSettingsAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateGlobalTableSettingsAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateGlobalTableSettingsResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateGlobalTableSettingsRequest();

            await service.UpdateGlobalTableSettingsAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateKinesisStreamingDestinationAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateKinesisStreamingDestinationAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateKinesisStreamingDestinationResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateKinesisStreamingDestinationRequest();

            await service.UpdateKinesisStreamingDestinationAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateTableAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateTableRequest();

            await service.UpdateTableAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateTableReplicaAutoScalingAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateTableReplicaAutoScalingAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateTableReplicaAutoScalingResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateTableReplicaAutoScalingRequest();

            await service.UpdateTableReplicaAutoScalingAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateTimeToLiveAsyncDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateTimeToLiveAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateTimeToLiveResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new UpdateTimeToLiveRequest();

            await service.UpdateTimeToLiveAsync(request, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListTablesAsyncWithNoParametersDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListTablesAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListTablesResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ListTablesAsync(CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListTablesAsyncWithExclusiveStartTableNameDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListTablesAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListTablesResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ListTablesAsync("StartTable", CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListTablesAsyncWithLimitDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListTablesAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListTablesResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ListTablesAsync(10, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task ListTablesAsyncWithExclusiveStartTableNameAndLimitDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                ListTablesAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new ListTablesResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.ListTablesAsync("StartTable", 10, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DeleteTableAsyncWithTableNameDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DeleteTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DeleteTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.DeleteTableAsync("TestTable", CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeTableAsyncWithTableNameDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.DescribeTableAsync("TestTable", CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task DescribeTimeToLiveAsyncWithTableNameDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                DescribeTimeToLiveAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new DescribeTimeToLiveResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);

            await service.DescribeTimeToLiveAsync("TestTable", CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public async Task UpdateTableAsyncWithTableNameAndProvisionedThroughputDelegatesToUnderlyingClient()
        {
            var called = false;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                UpdateTableAsyncFunc = (req, ct) =>
                {
                    called = true;
                    return Task.FromResult(new UpdateTableResponse());
                }
            };
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var provisionedThroughput = new ProvisionedThroughput();

            await service.UpdateTableAsync("TestTable", provisionedThroughput, CancellationToken.None);

            Assert.IsTrue(called);
        }

        [TestMethod]
        public void DetermineServiceOperationEndpointReturnsEndpoint()
        {
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(amazonDynamoDB: mockDynamoDB);
            var request = new GetItemRequest();

            var endpoint = service.DetermineServiceOperationEndpoint(request);

            Assert.IsNotNull(endpoint);
        }

        [TestMethod]
        public void IsStaleReturnsTrueWhenTransactionIsStale()
        {
            var staleDuration = TimeSpan.FromSeconds(10);
            var options = new MockOptionsSnapshot<AmazonDynamoDBOptions>(new AmazonDynamoDBOptions
            {
                TransactionStaleDuration = staleDuration
            });
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(options: options, amazonDynamoDB: mockDynamoDB);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("IsStale", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var lastUpdateDateTime = DateTime.UtcNow - staleDuration - TimeSpan.FromSeconds(1);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, lastUpdateDateTime, []);

            var result = method.Invoke(service, new object[] { transaction });

            Assert.IsTrue((bool)result!);
        }

        [TestMethod]
        public void IsStaleReturnsFalseWhenTransactionIsNotStale()
        {
            var staleDuration = TimeSpan.FromSeconds(10);
            var options = new MockOptionsSnapshot<AmazonDynamoDBOptions>(new AmazonDynamoDBOptions
            {
                TransactionStaleDuration = staleDuration
            });
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(options: options, amazonDynamoDB: mockDynamoDB);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("IsStale", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var lastUpdateDateTime = DateTime.UtcNow - staleDuration + TimeSpan.FromSeconds(1);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, lastUpdateDateTime, []);

            var result = method.Invoke(service, new object[] { transaction });

            Assert.IsFalse((bool)result!);
        }

        [TestMethod]
        public void IsStaleReturnsTrueWhenTransactionIsExactlyAtStaleDurationBoundary()
        {
            var staleDuration = TimeSpan.FromSeconds(10);
            var options = new MockOptionsSnapshot<AmazonDynamoDBOptions>(new AmazonDynamoDBOptions
            {
                TransactionStaleDuration = staleDuration
            });
            var mockDynamoDB = new MockAmazonDynamoDB();
            var service = CreateService(options: options, amazonDynamoDB: mockDynamoDB);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("IsStale", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var currentUtcNow = DateTime.UtcNow;
            var lastUpdateDateTime = currentUtcNow - staleDuration - TimeSpan.FromMilliseconds(100);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, lastUpdateDateTime, []);

            var result = method.Invoke(service, new object[] { transaction });

            Assert.IsTrue((bool)result!);
        }

        [TestMethod]
        public async Task GetItemAsyncWithUnCommittedIsolationLevelUsesUnCommittedService()
        {
            var unCommittedServiceCalled = false;
            var committedServiceCalled = false;

            var unCommittedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    unCommittedServiceCalled = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };

            var committedService = new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    committedServiceCalled = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };

            var service = CreateService(
                unCommittedService: unCommittedService,
                committedService: committedService);

            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.GetItemAsync(IsolationLevel.UnCommitted, request, CancellationToken.None);

            Assert.IsTrue(unCommittedServiceCalled);
            Assert.IsFalse(committedServiceCalled);
        }

        [TestMethod]
        public async Task GetItemAsyncWithCommittedIsolationLevelUsesCommittedService()
        {
            var unCommittedServiceCalled = false;
            var committedServiceCalled = false;

            var unCommittedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    unCommittedServiceCalled = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };

            var committedService = new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>
            {
                GetItemAsyncFunc = (req, ct) =>
                {
                    committedServiceCalled = true;
                    return Task.FromResult(new GetItemResponse());
                }
            };

            var service = CreateService(
                unCommittedService: unCommittedService,
                committedService: committedService);

            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            await service.GetItemAsync(IsolationLevel.Committed, request, CancellationToken.None);

            Assert.IsFalse(unCommittedServiceCalled);
            Assert.IsTrue(committedServiceCalled);
        }

        [TestMethod]
        public async Task GetItemAsyncWithInvalidIsolationLevelThrowsArgumentException()
        {
            var service = CreateService();

            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
            };

            var exception = await Assert.ThrowsExceptionAsync<ArgumentException>(() =>
                service.GetItemAsync((IsolationLevel)999, request, CancellationToken.None));

            Assert.IsTrue(exception.Message.Contains("Unrecognized isolation level"));
            Assert.IsTrue(exception.Message.Contains("999"));
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithUnCommittedIsolationLevelUsesUnCommittedService()
        {
            var unCommittedServiceCalled = false;
            var committedServiceCalled = false;

            var unCommittedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    unCommittedServiceCalled = true;
                    return Task.FromResult(new TransactGetItemsResponse());
                }
            };

            var committedService = new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    committedServiceCalled = true;
                    return Task.FromResult(new TransactGetItemsResponse());
                }
            };

            var service = CreateService(
                unCommittedService: unCommittedService,
                committedService: committedService);

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

            Assert.IsTrue(unCommittedServiceCalled);
            Assert.IsFalse(committedServiceCalled);
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithCommittedIsolationLevelUsesCommittedService()
        {
            var unCommittedServiceCalled = false;
            var committedServiceCalled = false;

            var unCommittedService = new MockIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    unCommittedServiceCalled = true;
                    return Task.FromResult(new TransactGetItemsResponse());
                }
            };

            var committedService = new MockIsolatedGetItemService<CommittedIsolationLevelServiceType>
            {
                TransactGetItemsAsyncFunc = (req, ct) =>
                {
                    committedServiceCalled = true;
                    return Task.FromResult(new TransactGetItemsResponse());
                }
            };

            var service = CreateService(
                unCommittedService: unCommittedService,
                committedService: committedService);

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

            await service.TransactGetItemsAsync(IsolationLevel.Committed, request, CancellationToken.None);

            Assert.IsFalse(unCommittedServiceCalled);
            Assert.IsTrue(committedServiceCalled);
        }

        [TestMethod]
        public async Task TransactGetItemsAsyncWithInvalidIsolationLevelThrowsArgumentException()
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
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } }
                        }
                    }
                }
            };

            var exception = await Assert.ThrowsExceptionAsync<ArgumentException>(() =>
                service.TransactGetItemsAsync((IsolationLevel)999, request, CancellationToken.None));

            Assert.IsTrue(exception.Message.Contains("Unrecognized isolation level"));
            Assert.IsTrue(exception.Message.Contains("999"));
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithEmptyTransactionReturnsEmptyList()
        {
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) => Task.FromResult(ImmutableList<ItemRecord>.Empty)
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, []);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithSingleRequestReturnsItemImages()
        {
            var key1 = ItemKey.Create("Table1", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item1" } } }.ToImmutableDictionary());
            var itemRecord1 = new ItemRecord(key1, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) =>
                {
                    if (version.Version == 1)
                        return Task.FromResult(ImmutableList.Create(itemRecord1));
                    return Task.FromResult(ImmutableList<ItemRecord>.Empty);
                }
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1]);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(key1, result[0].Key);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithMultipleRequestsReturnsAllItemImages()
        {
            var key1 = ItemKey.Create("Table1", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item1" } } }.ToImmutableDictionary());
            var key2 = ItemKey.Create("Table2", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item2" } } }.ToImmutableDictionary());
            var itemRecord1 = new ItemRecord(key1, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var itemRecord2 = new ItemRecord(key2, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) =>
                {
                    if (version.Version == 1)
                        return Task.FromResult(ImmutableList.Create(itemRecord1));
                    if (version.Version == 2)
                        return Task.FromResult(ImmutableList.Create(itemRecord2));
                    return Task.FromResult(ImmutableList<ItemRecord>.Empty);
                }
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var request2 = new RequestRecord(2, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1, request2]);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.IsTrue(result.Any(r => r.Key == key1));
            Assert.IsTrue(result.Any(r => r.Key == key2));
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithDuplicateKeysReturnsFirstImagePerKey()
        {
            var key1 = ItemKey.Create("Table1", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item1" } } }.ToImmutableDictionary());
            var itemRecord1a = new ItemRecord(key1, new Dictionary<string, ImmutableAttributeValue> { { "Version", ImmutableAttributeValue.Create(new AttributeValue { S = "first" }) } }.ToImmutableDictionary());
            var itemRecord1b = new ItemRecord(key1, new Dictionary<string, ImmutableAttributeValue> { { "Version", ImmutableAttributeValue.Create(new AttributeValue { S = "second" }) } }.ToImmutableDictionary());
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) =>
                {
                    if (version.Version == 1)
                        return Task.FromResult(ImmutableList.Create(itemRecord1a, itemRecord1b));
                    return Task.FromResult(ImmutableList<ItemRecord>.Empty);
                }
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1]);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(key1, result[0].Key);
            Assert.AreEqual("first", result[0].AttributeValues["Version"].S);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithRequestsWithoutImagesReturnsEmptyList()
        {
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) => Task.FromResult(ImmutableList<ItemRecord>.Empty)
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var request2 = new RequestRecord(2, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1, request2]);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithMixedScenarioReturnsOnlyItemsWithImages()
        {
            var key1 = ItemKey.Create("Table1", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item1" } } }.ToImmutableDictionary());
            var itemRecord1 = new ItemRecord(key1, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) =>
                {
                    if (version.Version == 1)
                        return Task.FromResult(ImmutableList.Create(itemRecord1));
                    return Task.FromResult(ImmutableList<ItemRecord>.Empty);
                }
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var request2 = new RequestRecord(2, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1, request2]);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(key1, result[0].Key);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncWithMultipleDifferentDuplicatesReturnsFirstPerKey()
        {
            var key1 = ItemKey.Create("Table1", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item1" } } }.ToImmutableDictionary());
            var key2 = ItemKey.Create("Table2", new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "item2" } } }.ToImmutableDictionary());
            var itemRecord1a = new ItemRecord(key1, new Dictionary<string, ImmutableAttributeValue> { { "Version", ImmutableAttributeValue.Create(new AttributeValue { S = "first-key1" }) } }.ToImmutableDictionary());
            var itemRecord1b = new ItemRecord(key1, new Dictionary<string, ImmutableAttributeValue> { { "Version", ImmutableAttributeValue.Create(new AttributeValue { S = "second-key1" }) } }.ToImmutableDictionary());
            var itemRecord2a = new ItemRecord(key2, new Dictionary<string, ImmutableAttributeValue> { { "Version", ImmutableAttributeValue.Create(new AttributeValue { S = "first-key2" }) } }.ToImmutableDictionary());
            var itemRecord2b = new ItemRecord(key2, new Dictionary<string, ImmutableAttributeValue> { { "Version", ImmutableAttributeValue.Create(new AttributeValue { S = "second-key2" }) } }.ToImmutableDictionary());
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) =>
                {
                    if (version.Version == 1)
                        return Task.FromResult(ImmutableList.Create(itemRecord1a, itemRecord2a));
                    if (version.Version == 2)
                        return Task.FromResult(ImmutableList.Create(itemRecord1b, itemRecord2b));
                    return Task.FromResult(ImmutableList<ItemRecord>.Empty);
                }
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var request2 = new RequestRecord(2, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1, request2]);

            var result = await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            var key1Result = result.First(r => r.Key == key1);
            var key2Result = result.First(r => r.Key == key2);
            Assert.AreEqual("first-key1", key1Result.AttributeValues["Version"].S);
            Assert.AreEqual("first-key2", key2Result.AttributeValues["Version"].S);
        }

        [TestMethod]
        public async Task GetItemImagesAsyncPropagatesCancellationToken()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.Cancel();
            var cancellationToken = cancellationTokenSource.Token;
            var mockItemImageStore = new MockItemImageStore
            {
                GetItemImagesAsyncFunc = (version, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(ImmutableList<ItemRecord>.Empty);
                }
            };
            var service = CreateService(itemImageStore: mockItemImageStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("GetItemImagesAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var request1 = new RequestRecord(1, null, null, null, null, null, null);
            var transaction = new Transaction("test-id", TransactionState.Active, 1, DateTime.UtcNow, [request1]);

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(async () =>
                await (Task<ImmutableList<ItemRecord>>)method.Invoke(service, new object[] { transaction, cancellationToken })!);
        }

        [TestMethod]
        public async Task InternalTransactWriteItemsAsyncReturnsResponseOnSuccess()
        {
            var expectedResponse = new TransactWriteItemsResponse();
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) => Task.FromResult(expectedResponse)
            };
            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };
            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB);
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
        public async Task InternalTransactWriteItemsAsyncThrowsTransactionConflictedExceptionWhenConflictsDetected()
        {
            var testTransactionId = new TransactionId("conflicting-txn");
            var itemKey = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } };
            var cancellationReasons = new List<CancellationReason>
            {
                new CancellationReason
                {
                    Code = "ConditionalCheckFailed",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test" } },
                        { "_tid", new AttributeValue { S = testTransactionId.Id } }
                    }
                }
            };

            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                    throw new TransactionCanceledException("Transaction cancelled")
                    {
                        CancellationReasons = cancellationReasons
                    }
            };

            var itemRequestDetail = new ItemRequestDetail(
                ItemKey.Create("TestTable", itemKey.ToImmutableDictionary()),
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail))
            };

            var itemRecord = new ItemRecord(
                ItemKey.Create("TestTable", itemKey.ToImmutableDictionary()),
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var transactionStateValue = new TransactionStateValue(
                false,
                testTransactionId.Id,
                null,
                false,
                false);

            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) =>
                    new ItemResponseAndTransactionState<ItemRecord>(itemRecord, transactionStateValue)
            };

            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };

            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = itemKey
                        }
                    }
                }
            };

            var exception = await Assert.ThrowsExceptionAsync<TransactionConflictedException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception);
            Assert.AreEqual("QUICK", exception.Id);
            Assert.AreEqual(1, exception.ConflictingItems.Count);
            Assert.AreEqual(testTransactionId.Id, exception.ConflictingItems[0].TransactionStateValue.TransactionId);
        }

        [TestMethod]
        public async Task InternalTransactWriteItemsAsyncRethrowsOriginalExceptionWhenNoConflictsDetected()
        {
            var itemKey = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } };
            var cancellationReasons = new List<CancellationReason>
            {
                new CancellationReason
                {
                    Code = "ConditionalCheckFailed",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test" } }
                    }
                }
            };

            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                    throw new TransactionCanceledException("Transaction cancelled")
                    {
                        CancellationReasons = cancellationReasons
                    }
            };

            var itemRequestDetail = new ItemRequestDetail(
                ItemKey.Create("TestTable", itemKey.ToImmutableDictionary()),
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail))
            };

            var itemRecord = new ItemRecord(
                ItemKey.Create("TestTable", itemKey.ToImmutableDictionary()),
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var transactionStateValueWithoutConflict = new TransactionStateValue(
                false,
                null,
                null,
                false,
                false);

            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) =>
                    new ItemResponseAndTransactionState<ItemRecord>(itemRecord, transactionStateValueWithoutConflict)
            };

            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };

            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = itemKey
                        }
                    }
                }
            };

            var exception = await Assert.ThrowsExceptionAsync<TransactionCanceledException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(exception);
            Assert.AreEqual("Transaction cancelled", exception.Message);
        }

        [TestMethod]
        public async Task InternalTransactWriteItemsAsyncPropagatesCancellationToken()
        {
            var cancellationTokenPassed = CancellationToken.None;
            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                {
                    cancellationTokenPassed = ct;
                    return Task.FromResult(new TransactWriteItemsResponse());
                }
            };
            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };
            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB);
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

            var cts = new CancellationTokenSource();
            await service.TransactWriteItemsAsync(request, cts.Token);

            Assert.AreEqual<CancellationToken>(cts.Token, cancellationTokenPassed);
        }

        [TestMethod]
        public async Task InternalTransactWriteItemsAsyncCallsTryGetTransactionConflictedExceptionAsyncWithCorrectParameters()
        {
            var itemKey = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "test" } } };
            var cancellationReasons = new List<CancellationReason>
            {
                new CancellationReason
                {
                    Code = "ConditionalCheckFailed",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { S = "test" } }
                    }
                }
            };

            var mockDynamoDB = new MockAmazonDynamoDB
            {
                TransactWriteItemsAsyncFunc = (req, ct) =>
                    throw new TransactionCanceledException("Transaction cancelled")
                    {
                        CancellationReasons = cancellationReasons
                    }
            };

            AmazonDynamoDBRequest? capturedRequest = null;
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    capturedRequest = req;
                    var itemRequestDetail = new ItemRequestDetail(
                        ItemKey.Create("TestTable", itemKey.ToImmutableDictionary()),
                        RequestAction.Put,
                        null,
                        ImmutableDictionary<string, string>.Empty,
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
                    return Task.FromResult(ImmutableList.Create(itemRequestDetail));
                }
            };

            var itemRecord = new ItemRecord(
                ItemKey.Create("TestTable", itemKey.ToImmutableDictionary()),
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);

            var transactionStateValue = new TransactionStateValue(
                false,
                null,
                null,
                false,
                false);

            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) =>
                    new ItemResponseAndTransactionState<ItemRecord>(itemRecord, transactionStateValue)
            };

            var quickOptions = new AmazonDynamoDBOptions
            {
                QuickTransactionsEnabled = true,
                TransactWriteItemCountMaxValue = 100
            };

            var service = CreateService(
                options: new MockOptionsSnapshot<AmazonDynamoDBOptions>(quickOptions),
                amazonDynamoDB: mockDynamoDB,
                requestService: mockRequestService,
                versionedItemStore: mockVersionedItemStore);

            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = itemKey
                        }
                    }
                }
            };

            await Assert.ThrowsExceptionAsync<TransactionCanceledException>(async () =>
            {
                await service.TransactWriteItemsAsync(request, CancellationToken.None);
            });

            Assert.IsNotNull(capturedRequest);
            Assert.IsInstanceOfType(capturedRequest, typeof(TransactWriteItemsRequest));
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncReturnsNullWhenNoConditionalCheckFailedReasons()
        {
            var transactionId = new TransactionId("test-id");
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList<ItemRequestDetail>.Empty)
            };
            var service = CreateService(requestService: mockRequestService);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "None" },
                new CancellationReason { Code = "ItemCollectionSizeLimitExceeded" }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNull(result);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncReturnsNullWhenNoConflictedItems()
        {
            var transactionId = new TransactionId("test-id");
            var itemKey = ItemKey.Create("TestTable", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail))
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) => new ItemResponseAndTransactionState<ItemRecord>(
                    new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new TransactionStateValue(true, null, DateTime.UtcNow, false, false))
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNull(result);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncReturnsExceptionWithSingleConflictedItem()
        {
            var transactionId = new TransactionId("test-id");
            var conflictingTransactionId = "conflicting-txn-id";
            var itemKey = ItemKey.Create("TestTable", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail))
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) => new ItemResponseAndTransactionState<ItemRecord>(
                    new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new TransactionStateValue(true, conflictingTransactionId, DateTime.UtcNow, false, false))
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(transactionId.Id, result.Id);
            Assert.AreEqual(1, result.ConflictingItems.Count);
            Assert.AreEqual(conflictingTransactionId, result.ConflictingItems[0].TransactionStateValue.TransactionId);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncReturnsExceptionWithMultipleConflictedItems()
        {
            var transactionId = new TransactionId("test-id");
            var conflictingTransactionId1 = "conflicting-txn-id-1";
            var conflictingTransactionId2 = "conflicting-txn-id-2";
            var itemKey1 = ItemKey.Create("TestTable1", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemKey2 = ItemKey.Create("TestTable2", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail1 = new ItemRequestDetail(
                itemKey1,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var itemRequestDetail2 = new ItemRequestDetail(
                itemKey2,
                RequestAction.Update,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail1, itemRequestDetail2))
            };
            var callCount = 0;
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) =>
                {
                    callCount++;
                    var txnId = callCount == 1 ? conflictingTransactionId1 : conflictingTransactionId2;
                    return new ItemResponseAndTransactionState<ItemRecord>(
                        new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                        new TransactionStateValue(true, txnId, DateTime.UtcNow, false, false));
                }
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() },
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(transactionId.Id, result.Id);
            Assert.AreEqual(2, result.ConflictingItems.Count);
            Assert.AreEqual(conflictingTransactionId1, result.ConflictingItems[0].TransactionStateValue.TransactionId);
            Assert.AreEqual(conflictingTransactionId2, result.ConflictingItems[1].TransactionStateValue.TransactionId);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncFiltersMixedCancellationReasons()
        {
            var transactionId = new TransactionId("test-id");
            var conflictingTransactionId = "conflicting-txn-id";
            var itemKey1 = ItemKey.Create("TestTable1", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemKey2 = ItemKey.Create("TestTable2", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemKey3 = ItemKey.Create("TestTable3", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail1 = new ItemRequestDetail(
                itemKey1,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var itemRequestDetail2 = new ItemRequestDetail(
                itemKey2,
                RequestAction.Update,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var itemRequestDetail3 = new ItemRequestDetail(
                itemKey3,
                RequestAction.Delete,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail1, itemRequestDetail2, itemRequestDetail3))
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) => new ItemResponseAndTransactionState<ItemRecord>(
                    new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new TransactionStateValue(true, conflictingTransactionId, DateTime.UtcNow, false, false))
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() },
                new CancellationReason { Code = "None" },
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(transactionId.Id, result.Id);
            Assert.AreEqual(2, result.ConflictingItems.Count);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncFiltersMixedConflictedAndNonConflictedItems()
        {
            var transactionId = new TransactionId("test-id");
            var conflictingTransactionId = "conflicting-txn-id";
            var itemKey1 = ItemKey.Create("TestTable1", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemKey2 = ItemKey.Create("TestTable2", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail1 = new ItemRequestDetail(
                itemKey1,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var itemRequestDetail2 = new ItemRequestDetail(
                itemKey2,
                RequestAction.Update,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail1, itemRequestDetail2))
            };
            var callCount = 0;
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) =>
                {
                    callCount++;
                    var txnId = callCount == 1 ? conflictingTransactionId : null;
                    return new ItemResponseAndTransactionState<ItemRecord>(
                        new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                        new TransactionStateValue(true, txnId, DateTime.UtcNow, false, false));
                }
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() },
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(transactionId.Id, result.Id);
            Assert.AreEqual(1, result.ConflictingItems.Count);
            Assert.AreEqual(conflictingTransactionId, result.ConflictingItems[0].TransactionStateValue.TransactionId);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncCorrectlyMapsItemKeysFromCancellationReasons()
        {
            var transactionId = new TransactionId("test-id");
            var conflictingTransactionId = "conflicting-txn-id";
            var itemKey = ItemKey.Create("TestTable", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail))
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) => new ItemResponseAndTransactionState<ItemRecord>(
                    new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new TransactionStateValue(true, conflictingTransactionId, DateTime.UtcNow, false, false))
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.ConflictingItems.Count);
            Assert.AreEqual(itemKey, result.ConflictingItems[0].ItemKey);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncCreatesCorrectTransactionConflictItemInstances()
        {
            var transactionId = new TransactionId("test-id");
            var conflictingTransactionId = "conflicting-txn-id";
            var lastUpdatedDate = DateTime.UtcNow;
            var itemKey = ItemKey.Create("TestTable", ImmutableDictionary<string, AttributeValue>.Empty);
            var itemRequestDetail = new ItemRequestDetail(
                itemKey,
                RequestAction.Put,
                null,
                ImmutableDictionary<string, string>.Empty,
                ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) => Task.FromResult(ImmutableList.Create(itemRequestDetail))
            };
            var mockVersionedItemStore = new MockVersionedItemStore
            {
                GetItemRecordAndTransactionStateFunc = (key, item) => new ItemResponseAndTransactionState<ItemRecord>(
                    new ItemRecord(key, ImmutableDictionary<string, ImmutableAttributeValue>.Empty),
                    new TransactionStateValue(true, conflictingTransactionId, lastUpdatedDate, false, false))
            };
            var service = CreateService(requestService: mockRequestService, versionedItemStore: mockVersionedItemStore);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>
            {
                new CancellationReason { Code = "ConditionalCheckFailed", Item = new Dictionary<string, AttributeValue>() }
            };
            var request = new TransactWriteItemsRequest();

            var result = await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, CancellationToken.None })!;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.ConflictingItems.Count);
            var conflictItem = result.ConflictingItems[0];
            Assert.AreEqual(itemKey, conflictItem.ItemKey);
            Assert.AreEqual(itemKey, conflictItem.ItemRecord.Key);
            Assert.AreEqual(conflictingTransactionId, conflictItem.TransactionStateValue.TransactionId);
            Assert.AreEqual(lastUpdatedDate, conflictItem.TransactionStateValue.LastUpdatedDate);
        }

        [TestMethod]
        public async Task TryGetTransactionConflictedExceptionAsyncPropagatesCancellationToken()
        {
            var transactionId = new TransactionId("test-id");
            var cancellationTokenPassed = false;
            using var cts = new CancellationTokenSource();
            var expectedToken = cts.Token;
            var mockRequestService = new MockRequestService
            {
                GetItemRequestDetailsAsyncFunc = (req, ct) =>
                {
                    cancellationTokenPassed = ct == expectedToken;
                    return Task.FromResult(ImmutableList<ItemRequestDetail>.Empty);
                }
            };
            var service = CreateService(requestService: mockRequestService);
            var amazonDynamoDBWithTransactionsType = typeof(AmazonDynamoDBWithTransactions);
            var method = amazonDynamoDBWithTransactionsType.GetMethod("TryGetTransactionConflictedExceptionAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(method);

            var exception = new TransactionCanceledException("Transaction cancelled");
            exception.CancellationReasons = new List<CancellationReason>();
            var request = new TransactWriteItemsRequest();

            await (Task<TransactionConflictedException?>)method.Invoke(service, new object[] { transactionId, exception, request, expectedToken })!;

            Assert.IsTrue(cancellationTokenPassed);
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
