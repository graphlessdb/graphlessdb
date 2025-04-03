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
using GraphlessDB;
using GraphlessDB.DynamoDB;
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Storage;
using GraphlessDB.Linq;
using GraphlessDB.Logging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Storage.Services.DynamoDB
{
    internal sealed class AmazonDynamoDBRDFTripleStore(
        IOptionsSnapshot<RDFTripleStoreOptions> options,
        IAmazonDynamoDBWithTransactions client,
        IAmazonDynamoDBRDFTripleItemService dataModelMapper,
        IRDFTripleStoreConsumedCapacity rdfTripleStoreConsumedCapacity,
        ILogger<AmazonDynamoDBRDFTripleStore> logger) : IRDFTripleStore<StoreType.Data>
    {
        public async Task<GetRDFTriplesResponse> GetRDFTriplesAsync(
           GetRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            if (request.Keys.IsEmpty)
            {
                return new GetRDFTriplesResponse([], RDFTripleStoreConsumedCapacity.None());
            }

            var responses = await Task.WhenAll(request
                .Keys
                .ToImmutableListBatches(100)
                .Select(keys => new GetRDFTriplesRequest(request.TableName, keys, request.ConsistentRead))
                .Select(async batchedRequest =>
                {
                    var dynamoDBRequest = ToDynamoDBRequest(batchedRequest);

                    var dynamoDBResponse = await BatchGetItemAsync(dynamoDBRequest, cancellationToken);

                    if (dynamoDBResponse.UnprocessedKeys.Count > 0)
                    {
                        throw new InvalidOperationException("Unprocessed keys");
                    }

                    return FromDynamoDBResponse(batchedRequest, dynamoDBResponse);
                }));

            if (responses.Length == 1)
            {
                return responses[0];
            }

            var aggregatedResponse = responses
                .Aggregate(
                    new GetRDFTriplesResponse([], RDFTripleStoreConsumedCapacity.None()),
                    (acc, cur) => new GetRDFTriplesResponse(acc.Items.AddRange(cur.Items), new RDFTripleStoreConsumedCapacity(
                        acc.ConsumedCapacity.CapacityUnits + cur.ConsumedCapacity.CapacityUnits,
                        acc.ConsumedCapacity.ReadCapacityUnits + cur.ConsumedCapacity.ReadCapacityUnits,
                        acc.ConsumedCapacity.WriteCapacityUnits + cur.ConsumedCapacity.WriteCapacityUnits)));

            return aggregatedResponse;
        }

        public async Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(
            WriteRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            return request.DisableTransaction
                ? await WriteRDFTriplesWithoutTransactionAsync(request, cancellationToken)
                : await WriteRDFTriplesWithTransactionAsync(request, cancellationToken);
        }

        public async Task RunHouseKeepingAsync(CancellationToken cancellationToken)
        {
            await client.RunHouseKeepingAsync(
                new RunHouseKeepingRequest(100, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5)),
                cancellationToken);
        }

        private async Task<WriteRDFTriplesResponse> WriteRDFTriplesWithoutTransactionAsync(
            WriteRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            if (request.Items.IsEmpty)
            {
                return new WriteRDFTriplesResponse(
                    RDFTripleStoreConsumedCapacity.None());
            }

            var dynamoDbRequests = ToBatchWriteItemRequests(request);
            var responses = await Task.WhenAll(dynamoDbRequests.Select(r => BatchWriteItemAsync(r, cancellationToken)));
            return new WriteRDFTriplesResponse(responses
                .SelectMany(r => r.ConsumedCapacity)
                .Aggregate(RDFTripleStoreConsumedCapacity.None(), (acc, cur) => Add(acc, FromConsumedCapacity(cur))));
        }


        private async Task<WriteRDFTriplesResponse> WriteRDFTriplesWithTransactionAsync(
            WriteRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            if (request.Items.IsEmpty)
            {
                return new WriteRDFTriplesResponse(
                    RDFTripleStoreConsumedCapacity.None());
            }

            var dynamoDbRequest = ToTransactWriteItemsRequest(request);
            var response = await TransactWriteItemsAsync(dynamoDbRequest, cancellationToken);
            return new WriteRDFTriplesResponse(response
                .ConsumedCapacity
                .Aggregate(RDFTripleStoreConsumedCapacity.None(), (acc, cur) => Add(acc, FromConsumedCapacity(cur))));
        }

        public async Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(
            ScanRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            var dynamoDBRequest = ToDynamoDBRequest(request);
            var dynamoDBResponse = await ScanAsync(dynamoDBRequest, cancellationToken);
            var response = FromDynamoDBResponse(dynamoDBResponse);
            rdfTripleStoreConsumedCapacity.AddConsumedCapacity(response.ConsumedCapacity);
            return response;
        }

        public async Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(
            QueryRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            var dynamoDbRequest = ToDynamoDBRequest(request);
            var dynamoDBResponse = await QueryAsync(dynamoDbRequest, cancellationToken);
            var response = FromDynamoDBResponse(dynamoDBResponse);
            rdfTripleStoreConsumedCapacity.AddConsumedCapacity(response.ConsumedCapacity);
            return response;
        }

        public async Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(
            QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
        {
            var dynamoDbRequest = ToDynamoDBRequest(request);
            var dynamoDBResponse = await QueryAsync(dynamoDbRequest, cancellationToken);
            var response = FromDynamoDBResponse(dynamoDBResponse);
            rdfTripleStoreConsumedCapacity.AddConsumedCapacity(response.ConsumedCapacity);
            return response;
        }

        private QueryRequest ToDynamoDBRequest(QueryRDFTriplesByPartitionAndPredicateRequest request)
        {
            return new QueryRequest
            {
                TableName = request.TableName,
                IndexName = GetByPredicateIndexName(request.TableName),
                KeyConditionExpression = $"#{nameof(RDFTriple.Partition)} = :{nameof(RDFTriple.Partition)} AND begins_with(#{nameof(RDFTriple.Predicate)}, :{nameof(RDFTriple.Predicate)})",
                ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { $"#{nameof(RDFTriple.Partition)}", nameof(RDFTriple.Partition) },
                        { $"#{nameof(RDFTriple.Predicate)}", nameof(RDFTriple.Predicate) }
                    },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { $":{nameof(RDFTriple.Partition)}", AttributeValueFactory.CreateS(request.Partition) },
                        { $":{nameof(RDFTriple.Predicate)}", AttributeValueFactory.CreateS(request.PredicateBeginsWith) }
                    },
                ExclusiveStartKey = request.ExclusiveStartKey != null
                    ? dataModelMapper.ToAttributeMap(request.ExclusiveStartKey)
                    : null,
                ScanIndexForward = request.ScanIndexForward,
                Limit = request.Limit,
                ConsistentRead = false, // NOT ALLOWED ON GLOBAL SECONDARY INDEXES
                ReturnConsumedCapacity = GetReturnConsumedCapacity()
            };
        }

        private QueryRequest ToDynamoDBRequest(QueryRDFTriplesRequest request)
        {
            return new QueryRequest
            {
                TableName = request.TableName,
                KeyConditionExpression = $"#{nameof(RDFTriple.Subject)} = :{nameof(RDFTriple.Subject)} AND begins_with(#{nameof(RDFTriple.Predicate)}, :{nameof(RDFTriple.Predicate)})",
                ExpressionAttributeNames = new Dictionary<string, string> {
                    { $"#{nameof(RDFTriple.Subject)}", nameof(RDFTriple.Subject) },
                    { $"#{nameof(RDFTriple.Predicate)}", nameof(RDFTriple.Predicate)}},
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>{
                    { $":{nameof(RDFTriple.Subject)}", AttributeValueFactory.CreateS(request.Subject)},
                    { $":{nameof(RDFTriple.Predicate)}", AttributeValueFactory.CreateS(request.PredicateBeginsWith)}},
                ExclusiveStartKey = request.ExclusiveStartKey != null
                    ? dataModelMapper.ToAttributeMap(request.ExclusiveStartKey)
                    : null,
                ScanIndexForward = request.ScanIndexForward,
                Limit = request.Limit,
                ConsistentRead = request.ConsistentRead,
                ReturnConsumedCapacity = GetReturnConsumedCapacity()
            };
        }

        private BatchGetItemRequest ToDynamoDBRequest(
            GetRDFTriplesRequest request)
        {
            return new BatchGetItemRequest
            {
                ReturnConsumedCapacity = GetReturnConsumedCapacity(),
                RequestItems = new Dictionary<string, KeysAndAttributes> {{
                    request.TableName,
                    new KeysAndAttributes {
                        ConsistentRead = request.ConsistentRead,
                        Keys = request
                            .Keys
                            .Select(dataModelMapper.ToAttributeMap)
                            .ToList()}}}
            };
        }

        private ScanRDFTriplesResponse FromDynamoDBResponse(
            ScanResponse dynamoDBResponse)
        {
            return new ScanRDFTriplesResponse(
                dynamoDBResponse
                    .Items
                    .Where(item => !IsTransient(item))
                    .Select(dataModelMapper.ToRDFTriple)
                    .ToImmutableList(),
                dynamoDBResponse.LastEvaluatedKey.Count > 0,
                FromConsumedCapacity(dynamoDBResponse.ConsumedCapacity));
        }

        private QueryRDFTriplesResponse FromDynamoDBResponse(
            QueryResponse dynamoDBResponse)
        {
            return new QueryRDFTriplesResponse(
                dynamoDBResponse
                    .Items
                    .Where(item => !IsTransient(item))
                    .Select(dataModelMapper.ToRDFTriple)
                    .ToImmutableList(),
                dynamoDBResponse.LastEvaluatedKey.Count > 0,
                FromConsumedCapacity(dynamoDBResponse.ConsumedCapacity));
        }

        private ScanRequest ToDynamoDBRequest(ScanRDFTriplesRequest request)
        {
            return new ScanRequest
            {
                TableName = request.TableName,
                Limit = request.Limit,
                ExclusiveStartKey = request.ExclusiveStartKey != null
                    ? dataModelMapper.ToAttributeMap(request.ExclusiveStartKey)
                    : null,
                ConsistentRead = request.ConsistentRead,
                ReturnConsumedCapacity = GetReturnConsumedCapacity()
            };
        }

        private static string GetByPredicateIndexName(string tableName)
        {
            return $"{tableName}ByPredicate";
        }

#pragma warning disable IDE0051 // Remove unused private members
        private static string GetByIndexedObjectIndexName(string tableName)
        {
            return $"{tableName}ByIndexedObject";
        }
#pragma warning restore IDE0051 // Remove unused private members

        private GetRDFTriplesResponse FromDynamoDBResponse(
            GetRDFTriplesRequest request, BatchGetItemResponse dynamoDBResponse)
        {
            var rdfTriplesByKey = dynamoDBResponse
                .Responses
                .SelectMany(response => response
                    .Value
                    .Where(item => !IsTransient(item))
                    .Select(item => dataModelMapper.ToRDFTriple(item)))
                .ToImmutableDictionary(k => k.AsKey());

            var items = request
                .Keys
                .Select(key =>
                {
                    rdfTriplesByKey.TryGetValue(new RDFTripleKey(key.Subject, key.Predicate), out var rdfTriple);
                    return rdfTriple;
                })
                .ToImmutableList();

            var consumedCapacity = dynamoDBResponse
                .ConsumedCapacity
                .Aggregate(RDFTripleStoreConsumedCapacity.None(), (acc, cur) => Add(acc, FromConsumedCapacity(cur)));

            return new GetRDFTriplesResponse(
                items,
                consumedCapacity);
        }

        private static ImmutableList<Tuple<TransactWriteItem, CancellationReason>> GetCancellationReasons(
            TransactionCanceledException exception, TransactWriteItemsRequest request)
        {
            return exception
                .CancellationReasons
                .Select((cr, i) =>
                {
                    return new
                    {
                        TransactionItem = request.TransactItems[i],
                        CancellationReason = cr
                    };
                })
                .Where(f => f.CancellationReason.Code != "None")
                .Select(f => new Tuple<TransactWriteItem, CancellationReason>(f.TransactionItem, f.CancellationReason))
                .ToImmutableList();
        }

        private TransactWriteItemsRequest ToTransactWriteItemsRequest(
            WriteRDFTriplesRequest request)
        {
            return new TransactWriteItemsRequest
            {
                ClientRequestToken = request.ClientRequestToken,
                ReturnConsumedCapacity = GetReturnConsumedCapacity(),
                TransactItems = request
                    .Items
                    .Select(ToTransactWriteItem)
                    .ToList(),
            };
        }

        private ImmutableList<BatchWriteItemRequest> ToBatchWriteItemRequests(WriteRDFTriplesRequest request)
        {
            return request
               .Items
               .ToImmutableListBatches(25)
               .Select(items => ToBatchWriteItemRequest(request with { Items = items }))
               .ToImmutableList();
        }

        private BatchWriteItemRequest ToBatchWriteItemRequest(WriteRDFTriplesRequest request)
        {

            return new BatchWriteItemRequest
            {
                ReturnConsumedCapacity = GetReturnConsumedCapacity(),
                RequestItems = request
                    .Items
                    .GroupBy(GetTableName)

                    .ToDictionary(k => k.Key, v => v.Select(ToWriteRequest).ToList())
            };
        }

        private string GetTableName(WriteRDFTriple value)
        {
            return value.Add?.TableName ??
                value.Update?.TableName ??
                value.Delete?.TableName ??
                value.UpdateAllEdgesVersion?.TableName ??
                value.IncrementAllEdgesVersion?.TableName ??
                value.CheckRDFTripleVersion?.TableName ??
                throw new InvalidOperationException();
        }

        private WriteRequest ToWriteRequest(WriteRDFTriple value)
        {
            if (value.Delete != null)
            {
                return new WriteRequest(
                    new DeleteRequest(
                        dataModelMapper.ToAttributeMap(value.Delete.Key)));
            }

            throw new NotSupportedException("Only Deletes without a transaction are supported");
        }

        private static bool IsTransient(Dictionary<string, AttributeValue> value)
        {
            return value.ContainsKey(ItemAttributeName.TRANSIENT.Value);
        }

        private TransactWriteItem ToTransactWriteItem(
            WriteRDFTriple value)
        {
            if (value.Add != null)
            {
                return new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = value.Add.TableName,
                        Item = dataModelMapper.ToAttributeMap(value.Add.Item),
                        ConditionExpression = GetConditionExpression(false, VersionDetailCondition.None),
                    }
                };
            }

            if (value.Update != null)
            {
                return new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = value.Update.TableName,
                        Item = dataModelMapper.ToAttributeMap(value.Update.Item),
                        ConditionExpression = GetConditionExpression(true, value.Update.VersionDetailCondition),
                        ExpressionAttributeNames = GetExpressionAttributeNames(value.Update.VersionDetailCondition)
                            .ToDictionary(),
                        ExpressionAttributeValues = GetExpressionAttributeValues(value.Update.VersionDetailCondition)
                            .ToDictionary(),
                    }
                };
            }

            if (value.Delete != null)
            {
                return new TransactWriteItem
                {
                    Delete = new Delete
                    {
                        TableName = value.Delete.TableName,
                        Key = dataModelMapper.ToAttributeMap(value.Delete.Key),
                        ConditionExpression = GetConditionExpression(true, value.Delete.VersionDetailCondition),
                        ExpressionAttributeNames = GetExpressionAttributeNames(value.Delete.VersionDetailCondition)
                            .ToDictionary(),
                        ExpressionAttributeValues = GetExpressionAttributeValues(value.Delete.VersionDetailCondition)
                            .ToDictionary(),
                    }
                };
            }

            if (value.IncrementAllEdgesVersion != null)
            {
                return new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = value.IncrementAllEdgesVersion.TableName,
                        Key = dataModelMapper.ToAttributeMap(value.IncrementAllEdgesVersion.Key),
                        UpdateExpression = $"SET #{nameof(RDFTriple.VersionDetail)}.#{nameof(VersionDetail.AllEdgesVersion)} = #{nameof(RDFTriple.VersionDetail)}.#{nameof(VersionDetail.AllEdgesVersion)} + :incrementValue",
                        ConditionExpression = GetConditionExpression(true, value.IncrementAllEdgesVersion.VersionDetailCondition),
                        ExpressionAttributeNames = GetExpressionAttributeNames(value.IncrementAllEdgesVersion.VersionDetailCondition, true)
                            .ToDictionary(),
                        ExpressionAttributeValues = GetExpressionAttributeValues(value.IncrementAllEdgesVersion.VersionDetailCondition)
                            .Add(":incrementValue", AttributeValueFactory.CreateN("1"))
                            .ToDictionary(),
                    }
                };
            }

            if (value.UpdateAllEdgesVersion != null)
            {
                if (!value.UpdateAllEdgesVersion.VersionDetailCondition.AllEdgesVersion.HasValue)
                {
                    throw new ArgumentException("AllEdgesVersion property must be set when using an UpdateAllEdgesVersion update");
                }

                return new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = value.UpdateAllEdgesVersion.TableName,
                        Key = dataModelMapper.ToAttributeMap(value.UpdateAllEdgesVersion.Key),
                        UpdateExpression = $"SET #{nameof(RDFTriple.VersionDetail)}.#{nameof(VersionDetail.AllEdgesVersion)} = :{nameof(VersionDetail.AllEdgesVersion)}_new",
                        ConditionExpression = GetConditionExpression(true, value.UpdateAllEdgesVersion.VersionDetailCondition),
                        ExpressionAttributeNames = GetExpressionAttributeNames(value.UpdateAllEdgesVersion.VersionDetailCondition, true)
                            .ToDictionary(),
                        ExpressionAttributeValues = GetExpressionAttributeValues(value.UpdateAllEdgesVersion.VersionDetailCondition)
                            .Add(
                                $":{nameof(VersionDetail.AllEdgesVersion)}_new",
                                AttributeValueFactory.CreateN((value.UpdateAllEdgesVersion.VersionDetailCondition.AllEdgesVersion.Value + 1).ToString(CultureInfo.InvariantCulture)))
                            .ToDictionary(),
                    }
                };
            }

            if (value.CheckRDFTripleVersion != null)
            {
                return new TransactWriteItem
                {
                    ConditionCheck = new ConditionCheck
                    {
                        TableName = value.CheckRDFTripleVersion.TableName,
                        Key = dataModelMapper.ToAttributeMap(value.CheckRDFTripleVersion.Key),
                        ConditionExpression = GetConditionExpression(true, value.CheckRDFTripleVersion.VersionDetailCondition),
                        ExpressionAttributeNames = GetExpressionAttributeNames(value.CheckRDFTripleVersion.VersionDetailCondition)
                            .ToDictionary(),
                        ExpressionAttributeValues = GetExpressionAttributeValues(value.CheckRDFTripleVersion.VersionDetailCondition)
                            .ToDictionary(),
                    }
                };
            }

            throw new NotSupportedException();
        }

        private static ImmutableDictionary<string, AttributeValue> GetExpressionAttributeValues(
            VersionDetailCondition versionDetailCondition)
        {
            var values = ImmutableDictionary<string, AttributeValue>.Empty;
            if (versionDetailCondition.NodeVersion.HasValue)
            {
                values = values.Add(
                    $":{nameof(VersionDetail.NodeVersion)}",
                    AttributeValueFactory.CreateN(versionDetailCondition.NodeVersion.Value.ToString(CultureInfo.InvariantCulture)));
            }

            if (versionDetailCondition.AllEdgesVersion.HasValue)
            {
                values = values.Add(
                    $":{nameof(VersionDetail.AllEdgesVersion)}",
                    AttributeValueFactory.CreateN(versionDetailCondition.AllEdgesVersion.Value.ToString(CultureInfo.InvariantCulture)));
            }

            return values;
        }

        private static ImmutableDictionary<string, string> GetExpressionAttributeNames(
            VersionDetailCondition versionDetailCondition,
            bool forceAddAllEdgesVersion = false)
        {
            var names = ImmutableDictionary<string, string>.Empty;
            if (forceAddAllEdgesVersion || versionDetailCondition.NodeVersion.HasValue || versionDetailCondition.AllEdgesVersion.HasValue)
            {
                names = names.Add($"#{nameof(RDFTriple.VersionDetail)}", nameof(RDFTriple.VersionDetail));
            }

            if (versionDetailCondition.NodeVersion.HasValue)
            {
                names = names.Add($"#{nameof(VersionDetail.NodeVersion)}", nameof(VersionDetail.NodeVersion));
            }

            if (forceAddAllEdgesVersion || versionDetailCondition.AllEdgesVersion.HasValue)
            {
                names = names.Add($"#{nameof(VersionDetail.AllEdgesVersion)}", nameof(VersionDetail.AllEdgesVersion));
            }

            return names;
        }

        private static string? GetConditionExpression(bool exists, VersionDetailCondition versionDetailCondition)
        {
            var conditions = new List<string>();
            if (exists)
            {
                conditions.Add($"attribute_exists({nameof(RDFTriple.Subject)})");
            }
            else
            {
                conditions.Add($"attribute_not_exists({nameof(RDFTriple.Subject)})");
            }

            if (versionDetailCondition.NodeVersion.HasValue)
            {
                conditions.Add($"#{nameof(RDFTriple.VersionDetail)}.#{nameof(VersionDetail.NodeVersion)} = :{nameof(VersionDetail.NodeVersion)}");
            }

            if (versionDetailCondition.AllEdgesVersion.HasValue)
            {
                conditions.Add($"#{nameof(RDFTriple.VersionDetail)}.#{nameof(VersionDetail.AllEdgesVersion)} = :{nameof(VersionDetail.AllEdgesVersion)}");
            }

            return conditions.Count == 0
                ? null
                : string.Join(" AND ", conditions);
        }

        private static string ToItemString(
            TransactWriteItem transactionItem)
        {
            if (transactionItem.ConditionCheck != null)
            {
                return string.Join(" ", transactionItem.ConditionCheck.Key.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            if (transactionItem.Put != null)
            {
                return string.Join(" ", transactionItem.Put.Item.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            if (transactionItem.Delete != null)
            {
                return string.Join(" ", transactionItem.Delete.Key.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            if (transactionItem.Update != null)
            {
                return string.Join(" ", transactionItem.Update.Key.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            throw new NotSupportedException("TransactionItem with no Put, Delete or Update not supported");
        }

        private static string ToConditionExpressionString(
            TransactWriteItem transactionItem)
        {
            if (transactionItem.ConditionCheck != null)
            {
                return transactionItem.ConditionCheck.ConditionExpression;
            }

            if (transactionItem.Put != null)
            {
                return transactionItem.Put.ConditionExpression;
            }

            if (transactionItem.Delete != null)
            {
                return transactionItem.Delete.ConditionExpression;
            }

            if (transactionItem.Update != null)
            {
                return transactionItem.Update.ConditionExpression;
            }

            throw new NotSupportedException("TransactionItem with no Put, Delete or Update not supported");
        }

        private static string ToConditionExpressionNamesString(
            TransactWriteItem transactionItem)
        {
            if (transactionItem.ConditionCheck != null)
            {
                return string.Join(" ", transactionItem.ConditionCheck.ExpressionAttributeNames.Select(i => $"{i.Key}:{i.Value}"));
            }

            if (transactionItem.Put != null)
            {
                return string.Join(" ", transactionItem.Put.ExpressionAttributeNames.Select(i => $"{i.Key}:{i.Value}"));
            }

            if (transactionItem.Delete != null)
            {
                return string.Join(" ", transactionItem.Delete.ExpressionAttributeNames.Select(i => $"{i.Key}:{i.Value}"));
            }

            if (transactionItem.Update != null)
            {
                return string.Join(" ", transactionItem.Update.ExpressionAttributeNames.Select(i => $"{i.Key}:{i.Value}"));
            }

            throw new NotSupportedException("TransactionItem with no Put, Delete or Update not supported");
        }

        private static string ToConditionExpressionValuesString(
            TransactWriteItem transactionItem)
        {
            if (transactionItem.ConditionCheck != null)
            {
                return string.Join(" ", transactionItem.ConditionCheck.ExpressionAttributeValues.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            if (transactionItem.Put != null)
            {
                return string.Join(" ", transactionItem.Put.ExpressionAttributeValues.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            if (transactionItem.Delete != null)
            {
                return string.Join(" ", transactionItem.Delete.ExpressionAttributeValues.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            if (transactionItem.Update != null)
            {
                return string.Join(" ", transactionItem.Update.ExpressionAttributeValues.Select(i => $"{i.Key}:{ToString(i.Value)}"));
            }

            throw new NotSupportedException("TransactionItem with no Put, Delete or Update not supported");
        }

        private static string ToString(
            AttributeValue value)
        {
            return value.S;
        }

        private ReturnConsumedCapacity GetReturnConsumedCapacity()
        {
            return options.Value.TrackConsumedCapacity
                ? ReturnConsumedCapacity.TOTAL
                : ReturnConsumedCapacity.NONE;
        }

        private static RDFTripleStoreConsumedCapacity Add(RDFTripleStoreConsumedCapacity a, RDFTripleStoreConsumedCapacity b)
        {
            return new RDFTripleStoreConsumedCapacity(
                a.CapacityUnits + b.CapacityUnits,
                a.ReadCapacityUnits + b.ReadCapacityUnits,
                a.WriteCapacityUnits + b.WriteCapacityUnits);
        }

        private static RDFTripleStoreConsumedCapacity FromConsumedCapacity(ConsumedCapacity? value)
        {
            return value == null
                ? RDFTripleStoreConsumedCapacity.None()
                : new RDFTripleStoreConsumedCapacity(value.CapacityUnits, value.ReadCapacityUnits, value.WriteCapacityUnits);
        }

        private Task<BatchGetItemResponse> BatchGetItemAsync(
            BatchGetItemRequest request, CancellationToken cancellationToken)
        {
            try
            {
                return client.BatchGetItemAsync(request, cancellationToken);
            }
            catch (InternalServerErrorException ex)
            {
                throw new GraphlessDBInternalServerErrorException(ex.Message, ex);
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                throw new GraphlessDBThroughputExceededException(ex.Message, ex);
            }
            catch (RequestLimitExceededException ex)
            {
                throw new GraphlessDBRequestLimitExceededException(ex.Message, ex);
            }
            catch (ResourceNotFoundException ex)
            {
                throw new GraphlessDBResourceNotFoundException(ex.Message, ex);
            }
        }

        private Task<BatchWriteItemResponse> BatchWriteItemAsync(
            BatchWriteItemRequest request, CancellationToken cancellationToken)
        {
            try
            {
                return client.BatchWriteItemAsync(request, cancellationToken);
            }
            catch (InternalServerErrorException ex)
            {
                throw new GraphlessDBInternalServerErrorException(ex.Message, ex);
            }
            catch (ItemCollectionSizeLimitExceededException ex)
            {
                throw new RequestSizeLimitExceededException(ex.Message, ex);
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                throw new GraphlessDBThroughputExceededException(ex.Message, ex);
            }
            catch (RequestLimitExceededException ex)
            {
                throw new GraphlessDBRequestLimitExceededException(ex.Message, ex);
            }
            catch (ResourceNotFoundException ex)
            {
                throw new GraphlessDBResourceNotFoundException(ex.Message, ex);
            }
        }

        private Task<TransactWriteItemsResponse> TransactWriteItemsAsync(
            TransactWriteItemsRequest request, CancellationToken cancellationToken)
        {
            try
            {
                return client.TransactWriteItemsAsync(request, cancellationToken);
            }
            catch (IdempotentParameterMismatchException ex)
            {
                throw new GraphlessDBIdempotentRequestMismatchException(ex.Message, ex);
            }
            catch (InternalServerErrorException ex)
            {
                throw new GraphlessDBInternalServerErrorException(ex.Message, ex);
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                throw new GraphlessDBThroughputExceededException(ex.Message, ex);
            }
            catch (RequestLimitExceededException ex)
            {
                throw new GraphlessDBRequestLimitExceededException(ex.Message, ex);
            }
            catch (ResourceNotFoundException ex)
            {
                throw new GraphlessDBResourceNotFoundException(ex.Message, ex);
            }
            catch (TransactionCanceledException ex)
            {
                var cancellationReasons = GetCancellationReasons(ex, request);

                cancellationReasons.ForEach(r => logger.EntityInTransactionCausedCancellation(
                    r.Item2.Code,
                    ToItemString(r.Item1),
                    ToConditionExpressionString(r.Item1),
                    ToConditionExpressionNamesString(r.Item1),
                    ToConditionExpressionValuesString(r.Item1)));

                throw new GraphlessDBConcurrencyException($"Failed to put entities due to out of date nodes and / or edges.", ex);
            }
        }

        private Task<ScanResponse> ScanAsync(
            ScanRequest request, CancellationToken cancellationToken)
        {
            try
            {
                return client.ScanAsync(request, cancellationToken);
            }
            catch (InternalServerErrorException ex)
            {
                throw new GraphlessDBInternalServerErrorException(ex.Message, ex);
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                throw new GraphlessDBThroughputExceededException(ex.Message, ex);
            }
            catch (RequestLimitExceededException ex)
            {
                throw new GraphlessDBRequestLimitExceededException(ex.Message, ex);
            }
            catch (ResourceNotFoundException ex)
            {
                throw new GraphlessDBResourceNotFoundException(ex.Message, ex);
            }
        }

        private Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken)
        {
            try
            {
                return client.QueryAsync(request, cancellationToken);
            }
            catch (InternalServerErrorException ex)
            {
                throw new GraphlessDBInternalServerErrorException(ex.Message, ex);
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                throw new GraphlessDBThroughputExceededException(ex.Message, ex);
            }
            catch (RequestLimitExceededException ex)
            {
                throw new GraphlessDBRequestLimitExceededException(ex.Message, ex);
            }
            catch (ResourceNotFoundException ex)
            {
                throw new GraphlessDBResourceNotFoundException(ex.Message, ex);
            }
        }
    }
}
