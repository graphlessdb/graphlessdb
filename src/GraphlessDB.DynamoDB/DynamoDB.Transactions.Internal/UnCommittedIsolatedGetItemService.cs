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
using GraphlessDB.DynamoDB.Transactions.Storage;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public class UnCommittedIsolatedGetItemService(
        IAmazonDynamoDB client,
        IVersionedItemStore versionedItemStore) : IIsolatedGetItemService<UnCommittedIsolationLevelServiceType>
    {
#pragma warning disable CS0649
        private static readonly bool s_reRouteRequestsToBatchGet;
        private static readonly bool s_reRouteRequestsToTransactGet;
#pragma warning restore CS0649

        public async Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken)
        {
            if (s_reRouteRequestsToBatchGet)
            {
                return await GetItemUsingBatchGetItemAsync(request, cancellationToken);
            }

            if (s_reRouteRequestsToTransactGet)
            {
                return await GetItemUsingTransactGetItemAsync(request, cancellationToken);
            }

            var getItemRequest = GetItemRequestWithAddedProjection(request);
            var getItemResponse = await client.GetItemAsync(getItemRequest, cancellationToken);
            var item = GetItemResponse(getItemResponse.Item);
            return new GetItemResponse
            {
                Item = item,
                IsItemSet = item.Count > 0
            };
        }

        private async Task<GetItemResponse> GetItemUsingTransactGetItemAsync(GetItemRequest request, CancellationToken cancellationToken)
        {
            var batchGetResponse = await TransactGetItemsAsync(new TransactGetItemsRequest
            {
                TransactItems = [
                    new() {
                        Get = new Get {
                            TableName = request.TableName,
                            Key = request.Key,
                            ProjectionExpression = request.ProjectionExpression,
                            ExpressionAttributeNames = request.ExpressionAttributeNames,
                        }
                    }
                ]
            }, cancellationToken);

            return new GetItemResponse
            {
                Item = GetItemResponse(batchGetResponse.Responses.Single().Item)
            };
        }

        private async Task<GetItemResponse> GetItemUsingBatchGetItemAsync(GetItemRequest request, CancellationToken cancellationToken)
        {
            var batchGetResponse = await BatchGetItemAsync(new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes> {
                        { request.TableName, new KeysAndAttributes {
                            ConsistentRead = request.ConsistentRead,
                            Keys = [request.Key],
                            ProjectionExpression = request.ProjectionExpression,
                            ExpressionAttributeNames = request.ExpressionAttributeNames }
                        }
                    }
            }, cancellationToken);

            var reroutedItem = GetItemResponse(batchGetResponse.Responses.Single().Value.Single());
            return new GetItemResponse
            {
                Item = reroutedItem,
                IsItemSet = reroutedItem.Count > 0
            };
        }

        public async Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken)
        {
            var batchGetItemRequest = new BatchGetItemRequest
            {
                RequestItems = request.RequestItems.ToDictionary(k => k.Key, v => GetKeysAndAttributesWithAddedProjection(v.Value)),
                ReturnConsumedCapacity = request.ReturnConsumedCapacity,
            };

            var batchGetResponse = await client.BatchGetItemAsync(batchGetItemRequest, cancellationToken);

            return new BatchGetItemResponse
            {
                Responses = batchGetResponse
                    .Responses
                    .Select(kv => new KeyValuePair<string, List<Dictionary<string, AttributeValue>>>(kv.Key, kv.Value.Select(vv => GetItemResponse(vv)).ToList()))
                    .ToDictionary(k => k.Key, v => v.Value),
                UnprocessedKeys = batchGetResponse.UnprocessedKeys
            };
        }

        public async Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken)
        {
            var transactGetItemsRequest = new TransactGetItemsRequest
            {
                TransactItems = request.TransactItems.Select(GetTransactItemWithAddedProjection).ToList(),
                ReturnConsumedCapacity = request.ReturnConsumedCapacity,
            };

            var transactGetItemsResponse = await client.TransactGetItemsAsync(transactGetItemsRequest, cancellationToken);

            return new TransactGetItemsResponse
            {
                Responses = transactGetItemsResponse
                    .Responses
                    .Select(r => new ItemResponse
                    {
                        Item = r.Item
                    })
                    .ToList()
            };
        }

        private static TransactGetItem GetTransactItemWithAddedProjection(TransactGetItem value)
        {
            var newValue = new TransactGetItem
            {
                Get = new Get
                {
                    TableName = value.Get.TableName,
                    Key = value.Get.Key,
                }
            };

            if (!string.IsNullOrWhiteSpace(value.Get.ProjectionExpression))
            {
                newValue.Get.ProjectionExpression = string.Join(", ", ItemAttributeName.Values.Select(v => $"#{v.Value}").Concat([value.Get.ProjectionExpression]));
                newValue.Get.ExpressionAttributeNames = value
                    .Get
                    .ExpressionAttributeNames
                    .Concat(ItemAttributeName.Values.Select(k => new KeyValuePair<string, string>($"#{k.Value}", k.Value)))
                    .ToDictionary(k => k.Key, v => v.Value);
            }

            return newValue;
        }

        private static GetItemRequest GetItemRequestWithAddedProjection(GetItemRequest request)
        {
            var getItemRequest = new GetItemRequest
            {
                TableName = request.TableName,
                Key = request.Key,
                ConsistentRead = request.ConsistentRead
            };

            if (!string.IsNullOrWhiteSpace(request.ProjectionExpression))
            {
                getItemRequest.ProjectionExpression = string.Join(", ", ItemAttributeName.Values.Select(v => $"#{v.Value}").Concat([request.ProjectionExpression]));
                getItemRequest.ExpressionAttributeNames = request
                       .ExpressionAttributeNames
                       .Concat(ItemAttributeName.Values.Select(k => new KeyValuePair<string, string>($"#{k.Value}", k.Value)))
                       .ToDictionary(k => k.Key, v => v.Value);
            }

            return getItemRequest;
        }

        private static KeysAndAttributes GetKeysAndAttributesWithAddedProjection(KeysAndAttributes keysAndAttributes)
        {
            var newKeysAndAttributes = new KeysAndAttributes
            {
                ConsistentRead = keysAndAttributes.ConsistentRead,
                Keys = keysAndAttributes.Keys
            };

            if (!string.IsNullOrWhiteSpace(keysAndAttributes.ProjectionExpression))
            {
                newKeysAndAttributes.ProjectionExpression = string.Join(", ", ItemAttributeName.Values.Select(v => $"#{v.Value}").Concat([keysAndAttributes.ProjectionExpression]));
                newKeysAndAttributes.ExpressionAttributeNames = keysAndAttributes
                       .ExpressionAttributeNames
                       .Concat(ItemAttributeName.Values.Select(k => new KeyValuePair<string, string>($"#{k.Value}", k.Value)))
                       .ToDictionary(k => k.Key, v => v.Value);
            }

            return newKeysAndAttributes;
        }

        private Dictionary<string, AttributeValue> GetItemResponse(Dictionary<string, AttributeValue> item)
        {
            // If the item doesn't exist, it's not locked
            if (item.Count == 0)
            {
                return item;
            }

            // If the item is transient, return a null item
            // But if the change is applied, return it even if it was a transient item (delete and lock do not apply)
            var (itemRecord, transactionState) = versionedItemStore.GetItemRecordAndTransactionState(item);
            if (transactionState.IsTransient && !transactionState.IsApplied)
            {
                return [];
            }

            return itemRecord;
        }
    }
}