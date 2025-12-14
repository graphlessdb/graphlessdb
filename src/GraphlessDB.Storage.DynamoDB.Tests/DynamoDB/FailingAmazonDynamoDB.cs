/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.Collections.Generic;
using GraphlessDB.DynamoDB.Transactions.Storage;
using GraphlessDB.DynamoDB.Transactions.Tests;
using Microsoft.Extensions.Options;

namespace GraphlessDB.DynamoDB
{
    public sealed class FailingAmazonDynamoDB(IOptions<FailingAmazonDynamoDBOptions> options, IAmazonDynamoDB dynamoDB) : IAmazonDynamoDB
    {
        public IDynamoDBv2PaginatorFactory Paginators => dynamoDB.Paginators;

        public IClientConfig Config => dynamoDB.Config;

        public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.BatchExecuteStatementAsync(request, cancellationToken);
        }

        public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default)
        {
            return dynamoDB.BatchGetItemAsync(requestItems, returnConsumedCapacity, cancellationToken);
        }

        public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default)
        {
            return dynamoDB.BatchGetItemAsync(requestItems, cancellationToken);
        }

        public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.BatchGetItemAsync(request, cancellationToken);
        }

        public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default)
        {
            return dynamoDB.BatchWriteItemAsync(requestItems, cancellationToken);
        }

        public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.BatchWriteItemAsync(request, cancellationToken);
        }

        public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.CreateBackupAsync(request, cancellationToken);
        }

        public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.CreateGlobalTableAsync(request, cancellationToken);
        }

        public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default)
        {
            return dynamoDB.CreateTableAsync(tableName, keySchema, attributeDefinitions, provisionedThroughput, cancellationToken);
        }

        public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.CreateTableAsync(request, cancellationToken);
        }

        public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteBackupAsync(request, cancellationToken);
        }

        public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteItemAsync(tableName, key, cancellationToken);
        }

        public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteItemAsync(tableName, key, returnValues, cancellationToken);
        }

        public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteItemAsync(request, cancellationToken);
        }

        public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteTableAsync(tableName, cancellationToken);
        }

        public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteTableAsync(request, cancellationToken);
        }

        public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeBackupAsync(request, cancellationToken);
        }

        public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeContinuousBackupsAsync(request, cancellationToken);
        }

        public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeContributorInsightsAsync(request, cancellationToken);
        }

        public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeEndpointsAsync(request, cancellationToken);
        }

        public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeExportAsync(request, cancellationToken);
        }

        public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeGlobalTableAsync(request, cancellationToken);
        }

        public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeGlobalTableSettingsAsync(request, cancellationToken);
        }

        public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeKinesisStreamingDestinationAsync(request, cancellationToken);
        }

        public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeLimitsAsync(request, cancellationToken);
        }

        public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeTableAsync(tableName, cancellationToken);
        }

        public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeTableAsync(request, cancellationToken);
        }

        public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeTableReplicaAutoScalingAsync(request, cancellationToken);
        }

        public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeTimeToLiveAsync(tableName, cancellationToken);
        }

        public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeTimeToLiveAsync(request, cancellationToken);
        }

        public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DisableKinesisStreamingDestinationAsync(request, cancellationToken);
        }

        public void Dispose()
        {
        }

        public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.EnableKinesisStreamingDestinationAsync(request, cancellationToken);
        }

        public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ExecuteStatementAsync(request, cancellationToken);
        }

        public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ExecuteTransactionAsync(request, cancellationToken);
        }

        public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ExportTableToPointInTimeAsync(request, cancellationToken);
        }

        public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default)
        {
            return GetItemAsync(new GetItemRequest
            {
                TableName = tableName,
                Key = key
            }, cancellationToken);
        }

        public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default)
        {
            return GetItemAsync(new GetItemRequest
            {
                TableName = tableName,
                Key = key,
                ConsistentRead = consistentRead,
            }, cancellationToken);
        }

        public async Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default)
        {
            if (options.Value.RequestsToFail.Contains(request))
            {
                throw new FailedYourRequestException();
            }

            var comparer = new FuncEqualityComparer<GetItemRequest>(AreEqual);
            if (options.Value.GetRequestsToTreatAsDeleted.Contains(request, comparer))
            {
                return new GetItemResponse();
            }

            var hasStubbedResults = options.Value.GetRequestsToStub.TryGetValue(request, out var stubbedResults);
            if (hasStubbedResults && stubbedResults != null && stubbedResults.Count > 0)
            {
                return stubbedResults.Dequeue();
            }

            return await dynamoDB.GetItemAsync(request, cancellationToken);
        }

        private static bool AreEqual(GetItemRequest? a, GetItemRequest? b)
        {
            if (a == null && b == null)
            {
                return true;
            }

            if (a == null || b == null)
            {
                return false;
            }

            // if (!a.AttributesToGet.SequenceEqual(b.AttributesToGet))
            // {
            //     return false;
            // }

            // if (a.ConsistentRead != b.ConsistentRead)
            // {
            //     return false;
            // }

            // if (!a.ExpressionAttributeNames.SequenceEqual(b.ExpressionAttributeNames))
            // {
            //     return false;
            // }

            var aKeys = a.Key.Keys.Order().ToImmutableList();
            var bKeys = b.Key.Keys.Order().ToImmutableList();
            var aValues = aKeys.Select(v => ImmutableAttributeValue.Create(a.Key[v])).ToImmutableList();
            var bValues = bKeys.Select(v => ImmutableAttributeValue.Create(b.Key[v])).ToImmutableList();
            if (aKeys.Count != bKeys.Count || !aKeys.SequenceEqual(bKeys) || !aValues.SequenceEqual(bValues))
            {
                return false;
            }

            // if (a.ProjectionExpression != b.ProjectionExpression)
            // {
            //     return false;
            // }

            // if (a.ReturnConsumedCapacity != b.ReturnConsumedCapacity)
            // {
            //     return false;
            // }

            if (a.TableName != b.TableName)
            {
                return false;
            }

            return true;
        }

        public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListBackupsAsync(request, cancellationToken);
        }

        public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListContributorInsightsAsync(request, cancellationToken);
        }

        public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListExportsAsync(request, cancellationToken);
        }

        public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListGlobalTablesAsync(request, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListTablesAsync(cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListTablesAsync(exclusiveStartTableName, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListTablesAsync(exclusiveStartTableName, limit, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListTablesAsync(limit, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListTablesAsync(request, cancellationToken);
        }

        public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListTagsOfResourceAsync(request, cancellationToken);
        }

        public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default)
        {
            return dynamoDB.PutItemAsync(tableName, item, cancellationToken);
        }

        public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = default)
        {
            return dynamoDB.PutItemAsync(tableName, item, returnValues, cancellationToken);
        }

        public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.PutItemAsync(request, cancellationToken);
        }

        public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.QueryAsync(request, cancellationToken);
        }

        public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.RestoreTableFromBackupAsync(request, cancellationToken);
        }

        public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.RestoreTableToPointInTimeAsync(request, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ScanAsync(tableName, attributesToGet, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ScanAsync(tableName, scanFilter, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ScanAsync(tableName, attributesToGet, scanFilter, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ScanAsync(request, cancellationToken);
        }

        public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.TagResourceAsync(request, cancellationToken);
        }

        public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.TransactGetItemsAsync(request, cancellationToken);
        }

        public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.TransactWriteItemsAsync(request, cancellationToken);
        }

        public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UntagResourceAsync(request, cancellationToken);
        }

        public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateContinuousBackupsAsync(request, cancellationToken);
        }

        public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateContributorInsightsAsync(request, cancellationToken);
        }

        public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateGlobalTableAsync(request, cancellationToken);
        }

        public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateGlobalTableSettingsAsync(request, cancellationToken);
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default)
        {
            return await UpdateItemAsync(
                new UpdateItemRequest
                {
                    TableName = tableName,
                    Key = key,
                    AttributeUpdates = attributeUpdates
                }, cancellationToken);
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default)
        {
            return await UpdateItemAsync(
                new UpdateItemRequest
                {
                    TableName = tableName,
                    Key = key,
                    AttributeUpdates = attributeUpdates,
                    ReturnValues = returnValues,
                }, cancellationToken);
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default)
        {
            if (options.Value.RequestsToFail.Contains(request))
            {
                throw new FailedYourRequestException();
            }

            return await dynamoDB.UpdateItemAsync(request, cancellationToken);
        }

        public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateTableAsync(tableName, provisionedThroughput, cancellationToken);
        }

        public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateTableAsync(request, cancellationToken);
        }

        public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateTableReplicaAutoScalingAsync(request, cancellationToken);
        }

        public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateTimeToLiveAsync(request, cancellationToken);
        }

        public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DescribeImportAsync(request, cancellationToken);
        }

        public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ImportTableAsync(request, cancellationToken);
        }

        public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.ListImportsAsync(request, cancellationToken);
        }

        public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request)
        {
            return dynamoDB.DetermineServiceOperationEndpoint(request);
        }

        public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.DeleteResourcePolicyAsync(request, cancellationToken);
        }

        public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.GetResourcePolicyAsync(request, cancellationToken);
        }

        public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.PutResourcePolicyAsync(request, cancellationToken);
        }

        public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return dynamoDB.UpdateKinesisStreamingDestinationAsync(request, cancellationToken);
        }
    }
}