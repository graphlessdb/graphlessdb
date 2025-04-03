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
using GraphlessDB.DynamoDB.Transactions.Storage;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public class CommittedIsolatedGetItemService(
        IAmazonDynamoDB client,
        ITransactionStore transactionStore,
        IVersionedItemStore versionedItemStore,
        IItemImageStore itemImageStore,
        IRequestService requestService) : IIsolatedGetItemService<CommittedIsolationLevelServiceType>
    {
#pragma warning disable CS0649
        private static readonly bool s_reRouteRequestsToBatchGet;
        private static readonly bool s_reRouteRequestsToTransactGet;
#pragma warning restore CS0649

        public async Task<GetItemResponse> GetItemAsync(
            GetItemRequest request,
            CancellationToken cancellationToken)
        {
            if (s_reRouteRequestsToBatchGet)
            {
                return await GetItemUsingBatchGetItemAsync(request, cancellationToken);
            }

            if (s_reRouteRequestsToTransactGet)
            {
                return await GetItemUsingTransactGetItemAsync(request, cancellationToken);
            }

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

            var getItemResponse = await client.GetItemAsync(getItemRequest, cancellationToken);
            return await GetItemResponseAsync(request.TableName, request.Key, getItemResponse.Item, cancellationToken);
        }

        public async Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken)
        {
            var batchGetItemRequest = new BatchGetItemRequest
            {
                RequestItems = request.RequestItems.ToDictionary(k => k.Key, v => GetKeysAndAttributesWithAddedProjection(v.Value)),
                ReturnConsumedCapacity = request.ReturnConsumedCapacity,
            };

            var batchGetResponse = await client.BatchGetItemAsync(batchGetItemRequest, cancellationToken);

            var itemRequestKeys = batchGetItemRequest
                .RequestItems
                .SelectMany(r => r.Value.Keys.Select(rr => new
                {
                    TableName = r.Key,
                    Key = rr
                }))
                .ToImmutableList();

            var itemRecordsAndTransactionStates = batchGetResponse
                .Responses
                .SelectMany(r => r.Value.Select(rr => new
                {
                    TableName = r.Key,
                    Item = rr
                }))
                .Select((tableNameAndItem, i) =>
                {
                    var key = itemRequestKeys[i].Key;
                    var (item, transactionState) = versionedItemStore.GetItemRecordAndTransactionState(tableNameAndItem.Item);
                    return new
                    {
                        tableNameAndItem.TableName,
                        Key = key,
                        Item = item,
                        TransactionState = transactionState
                    };
                })
                .ToImmutableList();

            var conflictingTransactions = await Task.WhenAll(itemRecordsAndTransactionStates
                .Where(t => t.TransactionState.TransactionId != null)
                .Select(t => transactionStore.GetAsync(new TransactionId(t.TransactionState.TransactionId ?? "Not possible"), true, cancellationToken)));

            var conflictingTransactionsById = conflictingTransactions.ToImmutableDictionary(k => k.Id);

            var conflictingItemRequestActions = await Task.WhenAll(conflictingTransactions
                .Select(t => requestService.GetItemRequestActionsAsync(t, cancellationToken)));

            var conflictingItemRequestActionsByTransactionId = conflictingItemRequestActions
                .Select((item, i) => new { item, i })
                .ToImmutableDictionary(k => conflictingTransactions[k.i].Id, v => v.item);

            var itemImages = await Task.WhenAll(itemRecordsAndTransactionStates
                .Where(t =>
                {
                    return IsItemImageRequired(t.TransactionState, conflictingTransactionsById);
                })
                .Select(async t =>
                {
                    var transactionId = t.TransactionState.TransactionId ?? "Not possible";
                    var itemRequestActions = conflictingItemRequestActionsByTransactionId[transactionId];
                    var itemKey = ItemKey.Create(t.TableName, t.Key.ToImmutableDictionary());
                    var lockingRequest = itemRequestActions.Where(r => r.Key == itemKey).First();
                    var transactionVersion = new TransactionVersion(transactionId, lockingRequest.RequestId);
                    var itemImages = await itemImageStore.GetItemImagesAsync(transactionVersion, cancellationToken);
                    return new KeyValuePair<TransactionVersion, ImmutableList<ItemRecord>>(transactionVersion, itemImages);
                }));

            var itemImagesByTransactionVersion = itemImages.ToImmutableDictionary(k => k.Key, v => v.Value);

            var responses = itemRecordsAndTransactionStates
                .Select(v => new
                {
                    v.TableName,
                    Response = GetItemResponse(
                        v.TableName,
                        v.Key,
                        v.Item,
                        v.TransactionState,
                        conflictingTransactionsById,
                        conflictingItemRequestActionsByTransactionId,
                        itemImagesByTransactionVersion)
                })
                .GroupBy(v => v.TableName)
                .ToDictionary(k => k.Key, v => v.Select(vv => vv.Response).ToList());

            return new BatchGetItemResponse
            {
                Responses = responses,
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

            var itemRequestKeys = transactGetItemsRequest
                .TransactItems
                .Select(r => new { r.Get.TableName, r.Get.Key })
                .ToImmutableList();

            var itemRecordsAndTransactionStates = transactGetItemsResponse
                .Responses
                .Select((response, i) =>
                {
                    var itemRequestKey = itemRequestKeys[i];
                    var (item, transactionState) = versionedItemStore.GetItemRecordAndTransactionState(response.Item);
                    return new
                    {
                        itemRequestKey.TableName,
                        itemRequestKey.Key,
                        Item = item,
                        TransactionState = transactionState
                    };
                })
                .ToImmutableList();

            var conflictingTransactions = await Task.WhenAll(itemRecordsAndTransactionStates
                .Where(t => t.TransactionState.TransactionId != null)
                .Select(t => transactionStore.GetAsync(new TransactionId(t.TransactionState.TransactionId ?? "Not possible"), true, cancellationToken)));

            var conflictingTransactionsById = conflictingTransactions.ToImmutableDictionary(k => k.Id);

            var conflictingItemRequestActions = await Task.WhenAll(conflictingTransactions
                .Select(t => requestService.GetItemRequestActionsAsync(t, cancellationToken)));

            var conflictingItemRequestActionsByTransactionId = conflictingItemRequestActions
                .Select((item, i) => new { item, i })
                .ToImmutableDictionary(k => conflictingTransactions[k.i].Id, v => v.item);

            var itemImages = await Task.WhenAll(itemRecordsAndTransactionStates
                .Where(t =>
                {
                    return IsItemImageRequired(t.TransactionState, conflictingTransactionsById);
                })
                .Select(async t =>
                {
                    var transactionId = t.TransactionState.TransactionId ?? "Not possible";
                    var itemRequestActions = conflictingItemRequestActionsByTransactionId[transactionId];
                    var itemKey = ItemKey.Create(t.TableName, t.Key.ToImmutableDictionary());
                    var lockingRequest = itemRequestActions.Where(r => r.Key == itemKey).First();
                    var transactionVersion = new TransactionVersion(transactionId, lockingRequest.RequestId);
                    var itemImages = await itemImageStore.GetItemImagesAsync(transactionVersion, cancellationToken);
                    return new KeyValuePair<TransactionVersion, ImmutableList<ItemRecord>>(transactionVersion, itemImages);
                }));

            var itemImagesByTransactionVersion = itemImages.ToImmutableDictionary(k => k.Key, v => v.Value);

            var responses = itemRecordsAndTransactionStates
                .Select(v => new ItemResponse
                {
                    Item = GetItemResponse(
                        v.TableName,
                        v.Key,
                        v.Item,
                        v.TransactionState,
                        conflictingTransactionsById,
                        conflictingItemRequestActionsByTransactionId,
                        itemImagesByTransactionVersion)
                })
                .ToList();

            return new TransactGetItemsResponse
            {
                Responses = responses,
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

            var batchGetItemResponse = new GetItemResponse
            {
                Item = batchGetResponse.Responses.Single().Value.Single()
            };

            return await GetItemResponseAsync(request.TableName, request.Key, batchGetItemResponse.Item, cancellationToken);
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

            var batchGetItemResponse = new GetItemResponse
            {
                Item = batchGetResponse.Responses.Single().Item
            };

            return await GetItemResponseAsync(request.TableName, request.Key, batchGetItemResponse.Item, cancellationToken);
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

        private static KeysAndAttributes GetKeysAndAttributesWithAddedProjection(KeysAndAttributes value)
        {
            var newValue = new KeysAndAttributes
            {
                ConsistentRead = value.ConsistentRead,
                Keys = value.Keys
            };

            if (!string.IsNullOrWhiteSpace(value.ProjectionExpression))
            {
                newValue.ProjectionExpression = string.Join(", ", ItemAttributeName.Values.Select(v => $"#{v.Value}").Concat([value.ProjectionExpression]));
                newValue.ExpressionAttributeNames = value
                       .ExpressionAttributeNames
                       .Concat(ItemAttributeName.Values.Select(k => new KeyValuePair<string, string>($"#{k.Value}", k.Value)))
                       .ToDictionary(k => k.Key, v => v.Value);
            }

            return newValue;
        }

        private static bool IsItemImageRequired(
            TransactionStateValue transactionState,
            ImmutableDictionary<string, Transaction> conflictingTransactionsById)
        {
            if (string.IsNullOrWhiteSpace(transactionState.TransactionId))
            {
                return false;
            }

            var lockingTx = conflictingTransactionsById[transactionState.TransactionId ?? "Not possible"];
            if (lockingTx.State is TransactionState.Committing or TransactionState.Committed)
            {
                return false;
            }

            if (transactionState.IsTransient)
            {
                return false;
            }

            return true;
        }

        private async Task<GetItemResponse> GetItemResponseAsync(
            string tableName,
            Dictionary<string, AttributeValue> key,
            Dictionary<string, AttributeValue> item,
            CancellationToken cancellationToken)
        {
            var (itemRecord, transactionState) = versionedItemStore
                .GetItemRecordAndTransactionState(item);

            var conflictingTransactionsById = transactionState.TransactionId != null
                ? ImmutableDictionary<string, Transaction>
                    .Empty
                    .Add(transactionState.TransactionId, await transactionStore.GetAsync(new TransactionId(transactionState.TransactionId ?? "Not possible"), true, cancellationToken))
                : ImmutableDictionary<string, Transaction>.Empty;

            var conflictingItemRequestActionsByTransactionId = transactionState.TransactionId != null
                ? ImmutableDictionary<string, ImmutableList<LockedItemRequestAction>>
                    .Empty
                    .Add(transactionState.TransactionId, await requestService.GetItemRequestActionsAsync(conflictingTransactionsById.First().Value, cancellationToken))
                : ImmutableDictionary<string, ImmutableList<LockedItemRequestAction>>.Empty;

            var itemImagesByTransactionVersion = ImmutableDictionary<TransactionVersion, ImmutableList<ItemRecord>>.Empty;
            if (IsItemImageRequired(transactionState, conflictingTransactionsById))
            {
                var transactionId = transactionState.TransactionId ?? "Not possible";
                var itemRequestActions = conflictingItemRequestActionsByTransactionId[transactionId];
                var itemKey = ItemKey.Create(tableName, key.ToImmutableDictionary());
                var lockingRequest = itemRequestActions.Where(r => r.Key == itemKey).First();
                var transactionVersion = new TransactionVersion(transactionId, lockingRequest.RequestId);
                var itemImages = await itemImageStore.GetItemImagesAsync(transactionVersion, cancellationToken);
                itemImagesByTransactionVersion = itemImagesByTransactionVersion.Add(transactionVersion, itemImages);
            }

            var itemResponse = GetItemResponse(
                tableName,
                key,
                itemRecord,
                transactionState,
                conflictingTransactionsById,
                conflictingItemRequestActionsByTransactionId,
                itemImagesByTransactionVersion);

            return new GetItemResponse
            {
                Item = itemResponse,
                IsItemSet = itemResponse.Count > 0,
            };
        }

        private Dictionary<string, AttributeValue> GetItemResponse(
            string tableName,
            Dictionary<string, AttributeValue> key,
            Dictionary<string, AttributeValue> item,
            TransactionStateValue transactionState,
            ImmutableDictionary<string, Transaction> conflictingTransactionsById,
            ImmutableDictionary<string, ImmutableList<LockedItemRequestAction>> conflictingItemRequestActionsByTransactionId,
            ImmutableDictionary<TransactionVersion, ImmutableList<ItemRecord>> itemImagesByTransactionVersion)
        {
            if (transactionState.TransactionId != null)
            {
                var lockingTx = conflictingTransactionsById[transactionState.TransactionId];
                if (lockingTx.State is TransactionState.Committing or TransactionState.Committed)
                {
                    var lockedItem = GetResponse(item, transactionState, lockingTx.State);
                    return lockedItem;
                }

                if (transactionState.IsTransient)
                {
                    return [];
                }

                var itemRequestActions = conflictingItemRequestActionsByTransactionId[transactionState.TransactionId];
                var itemKey = ItemKey.Create(tableName, key.ToImmutableDictionary());
                var lockingRequest = itemRequestActions.Where(r => r.Key == itemKey).First();
                if (lockingRequest.RequestAction == RequestAction.Delete)
                {
                    var (itemRecord, _) = versionedItemStore.GetItemRecordAndTransactionState(item);
                    return itemRecord;
                }

                var transactionVersion = new TransactionVersion(lockingTx.Id, lockingRequest.RequestId);
                var itemImages = itemImagesByTransactionVersion[transactionVersion];
                var itemImage = itemImages.Where(v => v.Key == itemKey).FirstOrDefault() ?? throw new TransactionException(lockingTx.Id, "Ran out of attempts to get a committed image of the item");

                return itemImage
                    .AttributeValues
                    .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue());
            }

            return GetResponse(item, transactionState, null);
        }

        private static Dictionary<string, AttributeValue> GetResponse(
            Dictionary<string, AttributeValue> item, TransactionStateValue itemTransactionState, TransactionState? transactionsState)
        {
            if (itemTransactionState.IsTransient)
            {
                return [];
            }

            if (!itemTransactionState.IsApplied)
            {
                return item;
            }

            if (itemTransactionState.TransactionId == null)
            {
                return item;
            }

            if (transactionsState.HasValue && (transactionsState == TransactionState.Committing || transactionsState == TransactionState.Committed))
            {
                return item;
            }

            throw new TransactionException(itemTransactionState.TransactionId, "Item has been modified in an uncommitted transaction.");
        }
    }
}