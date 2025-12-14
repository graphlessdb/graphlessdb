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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.Linq;
using Microsoft.Extensions.Options;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class VersionedItemStore(
        IOptionsSnapshot<AmazonDynamoDBOptions> options,
        IRequestService requestService,
        ITransactionServiceEvents transactionServiceEvents,
        ITransactionStore transactionStore,
        IAmazonDynamoDBKeyService amazonDynamoDBKeyService,
        IAmazonDynamoDB amazonDynamoDB) : IVersionedItemStore
    {
        public async Task<ImmutableDictionary<ItemKey, ItemTransactionState>> AcquireLocksAsync(
            Transaction transaction,
            AmazonDynamoDBRequest request,
            CancellationToken cancellationToken)
        {
            var onLockItemBeginAsync = transactionServiceEvents.OnAcquireLockAsync;
            if (onLockItemBeginAsync != null)
            {
                await onLockItemBeginAsync(transaction.GetId(), request, cancellationToken);
            }

            var lockItemRequestActions = await requestService.GetItemRequestActionsAsync(transaction, cancellationToken);
            var lockItemRequestActionsByKey = lockItemRequestActions.ToImmutableDictionary(k => k.Key);
            var itemRequestDetails = await requestService.GetItemRequestDetailsAsync(request, cancellationToken);
            var itemKeys = itemRequestDetails.Select(v => v.Key).ToImmutableList();
            var updateDate = DateTime.UtcNow;

            var itemTransactionStatesWithExists = itemRequestDetails
                    .Select(itemRequestDetail =>
                    {
                        var exists = GuessExists(itemRequestDetail);
                        return new Tuple<ItemTransactionState, bool>(
                            new ItemTransactionState(
                                itemRequestDetail.Key,
                                exists,
                                transaction.Id,
                                updateDate,
                                !exists,
                                false,
                                lockItemRequestActionsByKey[itemRequestDetail.Key])
                            , exists);
                    })
                    .ToImmutableList();

            try
            {
                var acquireLocksRequest = GetAcquireLocksRequest(itemTransactionStatesWithExists);
                await amazonDynamoDB.TransactWriteItemsAsync(acquireLocksRequest, cancellationToken);
                return itemTransactionStatesWithExists.ToImmutableDictionary(k => k.Item1.Key, v => v.Item1);
            }
            catch (TransactionCanceledException ex)
            {
                var (conflictedItems, failedItemsByKey) = GetConflictedItems(transaction, itemRequestDetails, ex);
                if (!conflictedItems.IsEmpty)
                {
                    throw new TransactionConflictedException(transaction.Id, conflictedItems);
                }

                try
                {
                    var itemTransactionStatesWithExistsAndCorrectedValues = GetCorrectedItemTransactionStates(itemTransactionStatesWithExists, failedItemsByKey);
                    var acquireLocksRequestAttemptTwo = GetAcquireLocksRequest(itemTransactionStatesWithExistsAndCorrectedValues);
                    await amazonDynamoDB.TransactWriteItemsAsync(acquireLocksRequestAttemptTwo, cancellationToken);
                    return itemTransactionStatesWithExistsAndCorrectedValues.ToImmutableDictionary(k => k.Item1.Key, v => v.Item1);
                }
                catch (TransactionCanceledException ex2)
                {
                    var (conflictedItems2, failedItemsByKey2) = GetConflictedItems(transaction, itemRequestDetails, ex2);
                    if (!conflictedItems.IsEmpty)
                    {
                        throw new TransactionConflictedException(transaction.Id, conflictedItems2);
                    }

                    throw new TransactionConflictedException(
                        transaction.Id, conflictedItems2, "Unable to acquire item lock for item", ex2);
                }
            }
        }

        private static ImmutableList<Tuple<ItemTransactionState, bool>> GetCorrectedItemTransactionStates(ImmutableList<Tuple<ItemTransactionState, bool>> itemTransactionStatesWithExists, ImmutableDictionary<ItemKey, ItemResponseAndTransactionState<ItemRecord>> failedItemsByKey)
        {
            return itemTransactionStatesWithExists
                .Select(itemTransactionStateWithExists =>
                {
                    if (failedItemsByKey.TryGetValue(itemTransactionStateWithExists.Item1.Key, out var failedItem))
                    {
                        var exists = !failedItem.ItemResponse.AttributeValues.IsEmpty || failedItem.TransactionStateValue.IsTransient;
                        var itemTransactionState = itemTransactionStateWithExists.Item1 with
                        {
                            IsApplied = failedItem.TransactionStateValue.IsApplied,
                            IsTransient = !exists || failedItem.TransactionStateValue.IsTransient,
                        };
                        return new Tuple<ItemTransactionState, bool>(itemTransactionState, exists);
                    }

                    return itemTransactionStateWithExists;
                })
                .ToImmutableList();
        }

        private Tuple<ImmutableList<TransactionConflictItem>, ImmutableDictionary<ItemKey, ItemResponseAndTransactionState<ItemRecord>>> GetConflictedItems(
            Transaction transaction,
            ImmutableList<ItemRequestDetail> itemRequestDetails,
            TransactionCanceledException ex)
        {
            var failedItems = ex
                .CancellationReasons
                .Select((c, i) =>
                {
                    if (c.Code == "ConditionalCheckFailed")
                    {
                        var itemKey = itemRequestDetails[i].Key;
                        return GetItemRecordAndTransactionState(itemKey, c.Item);
                    }

                    return null;
                })
                .WhereNotNull()
                .ToImmutableList();

            var failedItemsByKey = failedItems
                .ToImmutableDictionary(k => k.ItemResponse.Key);

            var conflictedItems = failedItems
                .Where(v => v.TransactionStateValue.TransactionId != null && v.TransactionStateValue.TransactionId != transaction.Id)
                .Select(v => new TransactionConflictItem(
                    v.ItemResponse.Key,
                    v.ItemResponse,
                    v.TransactionStateValue))
                .ToImmutableList();

            return new Tuple<ImmutableList<TransactionConflictItem>, ImmutableDictionary<ItemKey, ItemResponseAndTransactionState<ItemRecord>>>(
                conflictedItems, failedItemsByKey);
        }

        private static TransactWriteItemsRequest GetAcquireLocksRequest(ImmutableList<Tuple<ItemTransactionState, bool>> itemTransactionStatesWithExists)
        {
            return new TransactWriteItemsRequest
            {
                TransactItems = itemTransactionStatesWithExists
                    .Select(itemTransactionStateWithExists =>
                    {
                        var itemTransactionState = itemTransactionStateWithExists.Item1;
                        var exists = itemTransactionStateWithExists.Item2;
                        return GetAcquireLockRequest(
                            itemTransactionStateWithExists.Item1,
                            itemTransactionStateWithExists.Item2);
                    })
                    .ToList()
            };
        }

        public async Task<AmazonWebServiceResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            CancellationToken cancellationToken)
        {
            var onApplyRequestAsync = transactionServiceEvents.OnApplyRequestAsync;
            if (onApplyRequestAsync != null)
            {
                await onApplyRequestAsync(request.Transaction.GetId(), request.Request, cancellationToken);
            }

            return request.Request switch
            {
                PutItemRequest putItemRequest => await ApplyRequestAsync(request, putItemRequest, cancellationToken),
                UpdateItemRequest updateItemRequest => await ApplyRequestAsync(request, updateItemRequest, cancellationToken),
                DeleteItemRequest deleteItemRequest => ApplyRequest(request, deleteItemRequest),
                GetItemRequest getItemRequest => await ApplyRequestAsync(request, getItemRequest, cancellationToken),
                TransactGetItemsRequest transactGetItemsRequest => await ApplyRequestAsync(request, transactGetItemsRequest, cancellationToken),
                TransactWriteItemsRequest transactWriteItemsRequest => await ApplyRequestAsync(request, transactWriteItemsRequest, cancellationToken),
                _ => throw new NotSupportedException("Request type not supported"),
            };
        }

        public async Task ReleaseLocksAsync(
            Transaction transaction,
            bool rollback,
            ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey,
            CancellationToken cancellationToken)
        {
            var onReleaseLockAsync = transactionServiceEvents.OnReleaseLocksAsync;
            if (onReleaseLockAsync != null)
            {
                await onReleaseLockAsync(transaction.GetId(), rollback, cancellationToken);
            }

            var itemTransactionStates = await GetItemTransactionStatesAsync(transaction, cancellationToken);

            var releaseLocksRequest = new TransactWriteItemsRequest
            {
                TransactItems = itemTransactionStates
                    .Select(itemTransactionState =>
                    {
                        var itemKey = itemTransactionState.Key;
                        rollbackImagesByKey.TryGetValue(itemKey, out var rollbackImage);
                        return ToReleaseLockTransactWriteItem(
                            transaction.GetId(),
                            itemKey,
                            itemTransactionState,
                            rollback,
                            rollbackImage);
                    })
                    .WhereNotNull()
                    .ToList()
            };

            if (releaseLocksRequest.TransactItems.Count == 0)
            {
                return;
            }

            await BatchedTransactWriteItemsAsync(releaseLocksRequest, cancellationToken);
        }

        private async Task<TransactGetItemsResponse> BatchedTransactGetItemsAsync(
            TransactGetItemsRequest request,
            CancellationToken cancellationToken)
        {
            var responses = await Task.WhenAll(
                ToBatchedRequests(request, options.Value.TransactGetItemCountMaxValue)
                .Select(r => amazonDynamoDB.TransactGetItemsAsync(r, cancellationToken)));

            return new TransactGetItemsResponse
            {
                Responses = responses
                    .SelectMany(r => r.Responses)
                    .ToList()
            };
        }

        private async Task<TransactWriteItemsResponse> BatchedTransactWriteItemsAsync(
            TransactWriteItemsRequest request,
            CancellationToken cancellationToken)
        {
            var responses = await Task.WhenAll(
                ToBatchedRequests(request, options.Value.TransactWriteItemCountMaxValue)
                .Select(async r =>
                {
                    return await amazonDynamoDB.TransactWriteItemsAsync(r, cancellationToken);
                }));

            return new TransactWriteItemsResponse
            {
            };
        }

        private static ImmutableList<TransactGetItemsRequest> ToBatchedRequests(
            TransactGetItemsRequest source, int batchSize)
        {
            return source
                .TransactItems
                .ToListBatches(batchSize)
                .Select(batch => new TransactGetItemsRequest { TransactItems = batch })
                .ToImmutableList();
        }

        private static ImmutableList<TransactWriteItemsRequest> ToBatchedRequests(
            TransactWriteItemsRequest source, int batchSize)
        {
            return source
                .TransactItems
                .ToListBatches(batchSize)
                .Select(batch => new TransactWriteItemsRequest { TransactItems = batch })
                .ToImmutableList();
        }

        public async Task ReleaseLocksAsync(
            TransactionId id,
            TransactionId owningTransactionId,
            ImmutableList<ItemKey> itemKeys,
            bool rollback,
            ImmutableDictionary<ItemKey, ItemTransactionState> itemTransactionStatesByKey,
            ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey,
            CancellationToken cancellationToken)
        {
            if (!rollback)
            {
                throw new ArgumentException("Only rollback is supported with this method", nameof(rollback));
            }

            var onReleaseLockFromOtherTransactionAsync = transactionServiceEvents.OnReleaseLockFromOtherTransactionAsync;
            if (onReleaseLockFromOtherTransactionAsync != null)
            {
                await onReleaseLockFromOtherTransactionAsync(id, owningTransactionId, cancellationToken);
            }

            var releaseLocksRequest = new TransactWriteItemsRequest
            {
                TransactItems = itemKeys
                    .Select(itemKey =>
                    {
                        rollbackImagesByKey.TryGetValue(itemKey, out var rollbackImage);
                        var itemTransactionState = itemTransactionStatesByKey[itemKey];
                        return ToReleaseLockTransactWriteItem(
                            owningTransactionId,
                            itemKey,
                            itemTransactionState,
                            true,
                            rollbackImage);
                    })
                    .WhereNotNull()
                    .ToList()
            };

            if (releaseLocksRequest.TransactItems.Count == 0)
            {
                return;
            }

            await amazonDynamoDB.TransactWriteItemsAsync(releaseLocksRequest, cancellationToken);
        }

        public async Task<ImmutableList<ItemRecord>> GetItemsToBackupAsync(
            AmazonDynamoDBRequest request,
            CancellationToken cancellationToken)
        {
            var itemRequestDetails = await requestService.GetItemRequestDetailsAsync(request, cancellationToken);

            var itemKeysToFetch = itemRequestDetails
                .Where(itemRequestDetail => IsMutatingRequest(itemRequestDetail.RequestAction))
                .Select(itemRequestDetail => itemRequestDetail.Key)
                .ToImmutableList();

            if (itemKeysToFetch.IsEmpty)
            {
                return [];
            }

            var getItemsRequest = new TransactGetItemsRequest
            {
                TransactItems = itemKeysToFetch
                    .Select(itemKey => new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = itemKey.TableName,
                            Key = itemKey.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                        }
                    })
                    .ToList()
            };

            var getItemsResponse = await amazonDynamoDB.TransactGetItemsAsync(getItemsRequest, cancellationToken);

            var itemsAndTransactionStates = getItemsResponse
                .Responses
                .Select((r, i) =>
                {
                    var itemRecordAndTransState = GetItemRecordAndTransactionState(r.Item);
                    return new
                    {
                        ItemKey = itemKeysToFetch[i],
                        Item = itemRecordAndTransState.Item1,
                        TransactionState = itemRecordAndTransState.Item2,
                    };
                })
                .ToImmutableList();

            return itemsAndTransactionStates
                .Where(v => !(v.TransactionState.IsApplied || v.TransactionState.IsTransient))
                .Select(v => new ItemRecord(v.ItemKey, v.Item.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value))))
                .ToImmutableList();
        }

        private static bool IsMutatingRequest(RequestAction action)
        {
            return action switch
            {
                RequestAction.Put => true,
                RequestAction.Update => true,
                RequestAction.Delete => true,
                _ => false,
            };
        }

        private static bool GuessExists(ItemRequestDetail value)
        {
            return value.RequestAction switch
            {
                RequestAction.ConditionCheck => true,
                RequestAction.Delete => true,
                RequestAction.Get => true,
                RequestAction.Put => value.ConditionExpression?.Contains("attribute_exists") ?? false, // NOTE: A Put could update an entry so guess based on condition expression
                RequestAction.Update => true,
                _ => throw new NotSupportedException("Request action type is not supported"),
            };
        }

        public ItemResponseAndTransactionState<ItemRecord> GetItemRecordAndTransactionState(
            ItemKey itemKey,
            Dictionary<string, AttributeValue> item)
        {
            var transactionState = GetItemTransactionState(item);

            var itemRecord = transactionState.IsTransient && !transactionState.IsApplied
                ? new ItemRecord(itemKey, ImmutableDictionary<string, ImmutableAttributeValue>.Empty)
                : new ItemRecord(itemKey, item
                    .Where(kv => !ItemAttributeName.Values.Contains(new ItemAttributeName(kv.Key)))
                    .ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value)));

            return new ItemResponseAndTransactionState<ItemRecord>(
                itemRecord,
                transactionState);
        }

        public Tuple<Dictionary<string, AttributeValue>, TransactionStateValue> GetItemRecordAndTransactionState(
            Dictionary<string, AttributeValue> item)
        {
            var transactionState = GetItemTransactionState(item);

            var itemRecord = transactionState.IsTransient && !transactionState.IsApplied
                ? []
                : item
                    .Where(kv => !ItemAttributeName.Values.Contains(new ItemAttributeName(kv.Key)))
                    .ToDictionary(k => k.Key, v => v.Value);

            return new Tuple<Dictionary<string, AttributeValue>, TransactionStateValue>(
                itemRecord,
                transactionState);
        }

        private static Dictionary<string, AttributeValue> GetItemResponse(
            Dictionary<string, AttributeValue> item,
            ItemTransactionState transactionState,
            string? projectionExpression,
            Dictionary<string, string>? expressionAttributeNames)
        {
            if (transactionState.LockItemRequestAction.RequestAction == RequestAction.Delete)
            {
                // If the item we're getting is deleted in this transaction
                return [];
            }

            if (transactionState.LockItemRequestAction.RequestAction == RequestAction.Get && transactionState.IsTransient)
            {
                // If the item has only a read lock and is transient
                return [];
            }

            var projectedAttributeNames = projectionExpression
                ?.Split(",")
                .Select(v => v.Trim())
                .Select(v =>
                {
                    if (expressionAttributeNames != null && expressionAttributeNames.TryGetValue(v, out var exprName) && exprName != null)
                    {
                        return exprName;
                    }

                    return v;
                })
                .ToImmutableHashSet();

            return item
                .Where(kv => projectedAttributeNames == null || projectedAttributeNames.Contains(kv.Key))
                .ToDictionary(k => k.Key, v => v.Value);
        }

        private static TransactionStateValue GetItemTransactionState(Dictionary<string, AttributeValue> item)
        {
            item.TryGetValue(ItemAttributeName.TXID.Value, out var transactionId);
            item.TryGetValue(ItemAttributeName.DATE.Value, out var lastUpdatedDate);
            item.TryGetValue(ItemAttributeName.TRANSIENT.Value, out var isTransient);
            item.TryGetValue(ItemAttributeName.APPLIED.Value, out var isApplied);
            return new TransactionStateValue(
                item.Count > 0,
                transactionId?.S,
                lastUpdatedDate != null ? new DateTime(long.Parse(lastUpdatedDate.N, CultureInfo.InvariantCulture)) : null,
                isTransient != null,
                isApplied != null);
        }

        private async Task<ImmutableList<ItemTransactionState>> GetItemTransactionStatesAsync(
            Transaction transaction,
            CancellationToken cancellationToken)
        {
            var itemRequestActions = await requestService.GetItemRequestActionsAsync(transaction, cancellationToken);

            var transactGetItemsRequest = new TransactGetItemsRequest
            {
                TransactItems = itemRequestActions
                    .Select(itemRequestAction => new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = itemRequestAction.Key.TableName,
                            Key = itemRequestAction.Key.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                            // Fetch the Key attributes and the transaction attributes.  The key attributes are required to tell if a 'delete' operation has been fully committed
                            ProjectionExpression = string.Join(", ", itemRequestAction.Key.Key.Items.Select(i => i.Key).Concat(ItemAttributeName.Values.Select(v => $"#{v.Value}"))),
                            ExpressionAttributeNames = ItemAttributeName.Values.ToDictionary(k => $"#{k.Value}", v => v.Value)
                        }
                    })
                    .ToList()
            };

            if (transactGetItemsRequest.TransactItems.Count == 0)
            {
                return [];
            }

            var transactGetItemsResponse = await BatchedTransactGetItemsAsync(
                transactGetItemsRequest, cancellationToken);

            return transactGetItemsResponse
                .Responses
                .Select((response, i) =>
                {
                    var itemRequestAction = itemRequestActions[i];
                    return ItemTransactionState.Create(itemRequestAction.Key, GetItemTransactionState(response.Item), itemRequestAction);
                })
                .ToImmutableList();
        }

        private async Task<UpdateItemResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            UpdateItemRequest updateItemRequest,
            CancellationToken cancellationToken)
        {
            var itemKeys = await requestService.GetItemKeysAsync(updateItemRequest, cancellationToken);
            var itemKey = itemKeys.Single();
            var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];
            if (itemTransactionState.IsApplied)
            {
                if (updateItemRequest.ReturnValues == null || updateItemRequest.ReturnValues == ReturnValue.NONE)
                {
                    return new UpdateItemResponse
                    {
                        Attributes = []
                    };
                }

                if (updateItemRequest.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
                {
                    // No old values for transient item
                    return new UpdateItemResponse
                    {
                        Attributes = []
                    };
                }

                if (updateItemRequest.ReturnValues == ReturnValue.ALL_OLD)
                {
                    throw new NotSupportedException("ReturnValue.ALL_OLD is not supported");
                }

                if (updateItemRequest.ReturnValues == ReturnValue.ALL_NEW)
                {
                    var itemRecord = request.ItemsToBackupByKey[itemKey];
                    return new UpdateItemResponse
                    {
                        Attributes = itemRecord
                             .AttributeValues
                             .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                    };
                }

                throw new NotSupportedException();
            }

            updateItemRequest.ConditionExpression = string.Join(" AND ", new[] {
                $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}_expected",
                $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                updateItemRequest.ConditionExpression
                }.Where(s => !string.IsNullOrWhiteSpace(s)));

            updateItemRequest.UpdateExpression = AddSetStatement(
                updateItemRequest.UpdateExpression,
                $"#{ItemAttributeName.APPLIED.Value} = :{ItemAttributeName.APPLIED.Value}");

            updateItemRequest.ExpressionAttributeNames.Add($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value);
            updateItemRequest.ExpressionAttributeNames.Add($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value);

            updateItemRequest.ExpressionAttributeValues.Add($":{ItemAttributeName.TXID.Value}_expected", AttributeValueFactory.CreateS(request.Transaction.Id));
            updateItemRequest.ExpressionAttributeValues.Add($":{ItemAttributeName.APPLIED.Value}", AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal));

            var updateItemResponse = await amazonDynamoDB.UpdateItemAsync(updateItemRequest, cancellationToken);
            if (updateItemRequest.ReturnValues == null || updateItemRequest.ReturnValues == ReturnValue.NONE)
            {
                return new UpdateItemResponse
                {
                    Attributes = []
                };
            }

            if (updateItemRequest.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
            {
                // No old values for transient item
                return new UpdateItemResponse
                {
                    Attributes = []
                };
            }

            if (updateItemRequest.ReturnValues == ReturnValue.ALL_OLD)
            {
                var itemRecord = request.ItemsToBackupByKey[itemKey];
                return new UpdateItemResponse
                {
                    Attributes = itemRecord
                       .AttributeValues
                       .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                };
            }

            if (updateItemRequest.ReturnValues == ReturnValue.ALL_NEW)
            {
                var (responseItem, transactionState) = GetItemRecordAndTransactionState(updateItemResponse.Attributes);

                var itemResponse = GetItemResponse(
                    responseItem,
                    itemTransactionState,
                    null,
                    updateItemRequest.ExpressionAttributeNames);

                return new UpdateItemResponse
                {
                    Attributes = itemResponse
                };
            }

            throw new NotSupportedException();
        }

        private async Task<TransactGetItemsResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            TransactGetItemsRequest transactGetItemsRequest,
            CancellationToken cancellationToken)
        {
            var isGetRequireds = transactGetItemsRequest
                .TransactItems
                .Select(transactGetItem =>
                {
                    var itemKey = ItemKey.Create(transactGetItem.Get.TableName, transactGetItem.Get.Key.ToImmutableDictionary());
                    return new
                    {
                        ItemKey = itemKey,
                        IsGetRequired = IsGetRequired(itemKey, request),
                        TransactGetItem = transactGetItem
                    };
                })
                .ToImmutableList();

            var mustGets = isGetRequireds
                .Where(v => v.IsGetRequired)
                .ToImmutableList();

            var responses = ImmutableList<ItemResponse>.Empty;
            if (!mustGets.IsEmpty)
            {
                var transactGetItemsRequestReal = new TransactGetItemsRequest
                {
                    TransactItems = mustGets
                        .Select(v => v.TransactGetItem)
                        .ToList()
                };

                var responseReal = await amazonDynamoDB.TransactGetItemsAsync(
                    transactGetItemsRequestReal, cancellationToken);

                responses = [.. responseReal.Responses];
            }

            var itemsByKey = mustGets
                .Select((v, i) =>
                {
                    var response = responses[i];
                    var (itemRecord, _) = GetItemRecordAndTransactionState(response.Item);
                    return new
                    {
                        v.ItemKey,
                        ItemRecord = itemRecord,
                    };
                })
                .ToImmutableDictionary(k => k.ItemKey, v => v.ItemRecord);

            return new TransactGetItemsResponse
            {
                Responses = isGetRequireds
                    .Select(v => GetItemResponse(v.ItemKey, request, v.TransactGetItem, itemsByKey))
                    .ToList()
            };
        }

        private static bool IsGetRequired(
            ItemKey itemKey,
            ApplyRequestRequest request)
        {
            var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];
            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Delete)
            {
                return false;
            }

            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Get && itemTransactionState.IsTransient)
            {
                return false;
            }

            if (request.ItemsToBackupByKey.ContainsKey(itemKey))
            {
                return false;
            }

            return true;
        }

        private static ItemResponse GetItemResponse(
            ItemKey itemKey,
            ApplyRequestRequest request,
            TransactGetItem transactGetItem,
            ImmutableDictionary<ItemKey, Dictionary<string, AttributeValue>> itemsByKey)
        {
            var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];
            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Delete)
            {
                // If the item we're getting is deleted in this transaction
                return new ItemResponse
                {
                    Item = []
                };
            }

            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Get && itemTransactionState.IsTransient)
            {
                // If the item has only a read lock and is transient
                return new ItemResponse
                {
                    Item = []
                };
            }

            if (request.ItemsToBackupByKey.TryGetValue(itemKey, out var item))
            {
                var projectedAttributeNames = transactGetItem
                    .Get
                    .ProjectionExpression
                    ?.Split(",")
                    .Select(v => v.Trim())
                    .Select(v =>
                    {
                        if (transactGetItem.Get.ExpressionAttributeNames.TryGetValue(v, out var exprName))
                        {
                            return exprName;
                        }

                        return v;
                    })
                    .ToImmutableHashSet();

                return new ItemResponse
                {
                    Item = item
                        .AttributeValues
                        .Where(kv =>
                            !ItemAttributeName.Values.Contains(new ItemAttributeName(kv.Key)) &&
                            (projectedAttributeNames == null || projectedAttributeNames.Contains(kv.Key)))
                        .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                };
            }

            return new ItemResponse
            {
                Item = itemsByKey[itemKey]
            };
        }

        private async Task<TransactWriteItemsResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            TransactWriteItemsRequest transactWriteItemsRequest,
            CancellationToken cancellationToken)
        {
            var itemRequestDetails = await requestService.GetItemRequestDetailsAsync(transactWriteItemsRequest, cancellationToken);

            var unlockedItemsExist = request.ItemTransactionStatesByKey
                .Values
                .Where(v => v.TransactionId != request.Transaction.Id)
                .Any();

            if (unlockedItemsExist)
            {
                await transactionStore.GetAsync(request.Transaction.GetId(), true, cancellationToken);
                throw new InvalidOperationException("All items should be locked");
            }

            var itemRequestDetailsByKey = itemRequestDetails.ToImmutableDictionary(k => k.Key);

            var updatedTransactWriteItems = await Task.WhenAll(transactWriteItemsRequest
                .TransactItems
                .Select(async transactWriteItem =>
                {
                    if (transactWriteItem.Delete != null)
                    {
                        return null;
                    }

                    if (transactWriteItem.ConditionCheck != null)
                    {
                        if (IsSupportedConditionExpression(transactWriteItem.ConditionCheck, "attribute_not_exists"))
                        {
                            return new TransactWriteItem
                            {
                                ConditionCheck = new ConditionCheck
                                {
                                    TableName = transactWriteItem
                                        .ConditionCheck
                                        .TableName,
                                    Key = transactWriteItem
                                        .ConditionCheck
                                        .Key
                                        .ToDictionary(k => k.Key, v => Clone(v.Value)),
                                    ConditionExpression = Combine(
                                        $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}",
                                        $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                                        $"attribute_exists(#{ItemAttributeName.TRANSIENT.Value})"),
                                    ExpressionAttributeNames = transactWriteItem
                                        .ConditionCheck
                                        .ExpressionAttributeNames
                                        .Concat([
                                            new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                            new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                            new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                                         ])
                                        .ToDictionary(k => k.Key, v => v.Value),
                                    ExpressionAttributeValues = transactWriteItem
                                        .ConditionCheck
                                        .ExpressionAttributeValues
                                        .Select(kv => new KeyValuePair<string, AttributeValue>(kv.Key, Clone(kv.Value)))
                                        .Concat([
                                            new KeyValuePair<string, AttributeValue>($":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(request.Transaction.Id))
                                         ])
                                        .ToDictionary(k => k.Key, v => v.Value),
                                    ReturnValuesOnConditionCheckFailure = transactWriteItem.ConditionCheck.ReturnValuesOnConditionCheckFailure,
                                }
                            };
                        }

                        if (IsSupportedConditionExpression(transactWriteItem.ConditionCheck, "attribute_exists"))
                        {
                            return new TransactWriteItem
                            {
                                ConditionCheck = new ConditionCheck
                                {
                                    TableName = transactWriteItem
                                        .ConditionCheck
                                        .TableName,
                                    Key = transactWriteItem
                                        .ConditionCheck
                                        .Key
                                        .ToDictionary(k => k.Key, v => Clone(v.Value)),
                                    ConditionExpression = Combine(
                                        $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}",
                                        $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                                        $"attribute_not_exists(#{ItemAttributeName.TRANSIENT.Value})"),
                                    ExpressionAttributeNames = transactWriteItem
                                        .ConditionCheck
                                        .ExpressionAttributeNames
                                        .Concat([
                                            new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                            new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                            new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                                         ])
                                        .ToDictionary(k => k.Key, v => v.Value),
                                    ExpressionAttributeValues = transactWriteItem
                                        .ConditionCheck
                                        .ExpressionAttributeValues
                                        .Select(kv => new KeyValuePair<string, AttributeValue>(kv.Key, Clone(kv.Value)))
                                        .Concat([
                                            new KeyValuePair<string, AttributeValue>($":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(request.Transaction.Id))
                                         ])
                                        .ToDictionary(k => k.Key, v => v.Value),
                                    ReturnValuesOnConditionCheckFailure = transactWriteItem.ConditionCheck.ReturnValuesOnConditionCheckFailure,
                                }
                            };
                        }

                        throw new NotSupportedException("ConditionExpression format not supported");
                    }

                    if (transactWriteItem.Put != null)
                    {
                        var itemKeyAttributes = await amazonDynamoDBKeyService.CreateKeyMapAsync(
                            transactWriteItem.Put.TableName,
                            transactWriteItem.Put.Item.ToImmutableDictionary(),
                            cancellationToken);

                        var itemKey = ItemKey.Create(
                            transactWriteItem.Put.TableName,
                            itemKeyAttributes);

                        var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];

                        var itemRequestDetail = itemRequestDetailsByKey[itemKey];

                        if (itemTransactionState.IsApplied)
                        {
                            return new TransactWriteItem
                            {
                                ConditionCheck = new ConditionCheck
                                {
                                    TableName = itemKey
                                        .TableName,
                                    Key = itemKey
                                        .Key
                                        .Items
                                        .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                                    ConditionExpression = $"#{ItemAttributeName.TXID.Value}_expected = :{ItemAttributeName.TXID.Value} AND attribute_exists(#{ItemAttributeName.APPLIED.Value}_expected)",
                                    ExpressionAttributeNames = new Dictionary<string, string> {
                                        {$"#{ItemAttributeName.TXID.Value}_expected", ItemAttributeName.TXID.Value},
                                        {$"#{ItemAttributeName.APPLIED.Value}_expected", ItemAttributeName.APPLIED.Value}
                                    },
                                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                                        {$":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(request.Transaction.Id)},
                                    }
                                }
                            };
                        }

                        var putItem = transactWriteItem
                            .Put
                            .Item
                            .Select(k => new KeyValuePair<string, AttributeValue>(k.Key, Clone(k.Value)))
                            .Concat([
                                new KeyValuePair<string, AttributeValue>(ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(request.Transaction.Id)),
                                new KeyValuePair<string, AttributeValue>(ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal)),
                                new KeyValuePair<string, AttributeValue>(ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture))),
                            ])
                            .ToDictionary(k => k.Key, v => v.Value);

                        if (itemTransactionState.IsTransient)
                        {
                            putItem.Add(ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal));
                        }

                        return new TransactWriteItem
                        {
                            Put = new Put
                            {
                                TableName = transactWriteItem
                                    .Put
                                    .TableName,
                                Item = putItem,
                                ConditionExpression = CombinePreLockCondition(
                                    $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value} AND attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                                    itemRequestDetail,
                                    itemTransactionState),
                                ExpressionAttributeNames = transactWriteItem
                                    .Put
                                    .ExpressionAttributeNames
                                    .Concat([
                                        new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                        new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                    ])
                                    .ToDictionary(k => k.Key, v => v.Value),
                                ExpressionAttributeValues = transactWriteItem
                                        .Put
                                        .ExpressionAttributeValues
                                        .Select(kv => new KeyValuePair<string, AttributeValue>(kv.Key, Clone(kv.Value)))
                                        .Concat([
                                            new KeyValuePair<string, AttributeValue>($":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(request.Transaction.Id))
                                         ])
                                        .ToDictionary(k => k.Key, v => v.Value),
                                ReturnValuesOnConditionCheckFailure = transactWriteItem.Put.ReturnValuesOnConditionCheckFailure,
                            }
                        };
                    }

                    if (transactWriteItem.Update != null)
                    {
                        var itemKey = ItemKey.Create(
                            transactWriteItem.Update.TableName,
                            transactWriteItem.Update.Key.ToImmutableDictionary());

                        var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];
                        if (itemTransactionState.IsApplied)
                        {
                            return new TransactWriteItem
                            {
                                ConditionCheck = new ConditionCheck
                                {
                                    TableName = itemKey
                                        .TableName,
                                    Key = itemKey
                                        .Key
                                        .Items
                                        .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                                    ConditionExpression = $"#{ItemAttributeName.TXID.Value}_expected = :{ItemAttributeName.TXID.Value} AND attribute_exists(#{ItemAttributeName.APPLIED.Value}_expected)",
                                    ExpressionAttributeNames = new Dictionary<string, string> {
                                        {$"#{ItemAttributeName.TXID.Value}_expected", ItemAttributeName.TXID.Value},
                                        {$"#{ItemAttributeName.APPLIED.Value}_expected", ItemAttributeName.APPLIED.Value}
                                    },
                                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                                        {$":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(request.Transaction.Id)},
                                    }
                                }
                            };
                        }

                        return new TransactWriteItem
                        {
                            Update = new Update
                            {
                                TableName = transactWriteItem
                                    .Update
                                    .TableName,
                                Key = itemKey
                                    .Key
                                    .Items
                                    .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                                ConditionExpression = string.Join(" AND ",
                                    new[] {
                                        $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}_expected",
                                        $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                                        transactWriteItem.Update.ConditionExpression
                                    }.Where(s => !string.IsNullOrWhiteSpace(s))),
                                UpdateExpression = AddSetStatement(
                                    transactWriteItem.Update.UpdateExpression,
                                    $"#{ItemAttributeName.APPLIED.Value} = :{ItemAttributeName.APPLIED.Value}"),
                                ExpressionAttributeNames = transactWriteItem
                                    .Update
                                    .ExpressionAttributeNames
                                    .Concat([
                                        new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                        new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                    ])
                                    .ToDictionary(k => k.Key, v => v.Value),
                                ExpressionAttributeValues = transactWriteItem
                                        .Update
                                        .ExpressionAttributeValues
                                        .Select(kv => new KeyValuePair<string, AttributeValue>(kv.Key, Clone(kv.Value)))
                                        .Concat([
                                            new KeyValuePair<string, AttributeValue>($":{ItemAttributeName.TXID.Value}_expected", AttributeValueFactory.CreateS(request.Transaction.Id)),
                                            new KeyValuePair<string, AttributeValue>($":{ItemAttributeName.APPLIED.Value}", AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal))
                                         ])
                                        .ToDictionary(k => k.Key, v => v.Value),
                                ReturnValuesOnConditionCheckFailure = transactWriteItem
                                        .Update
                                        .ReturnValuesOnConditionCheckFailure,
                            }
                        };
                    }

                    throw new NotSupportedException();
                }));

            var applyRequest = new TransactWriteItemsRequest
            {
                TransactItems = updatedTransactWriteItems
                    .Where(v => v != null)
                    .ToList()
            };

            if (applyRequest.TransactItems.Count == 0)
            {
                // There may not be any requests to actually process if they were deletes
                return new TransactWriteItemsResponse();
            }

            return await amazonDynamoDB.TransactWriteItemsAsync(applyRequest, cancellationToken);
        }

        private static bool IsSupportedConditionExpression(ConditionCheck conditionCheck, string conditionExpressionFunction)
        {
            return conditionCheck.Key.Keys.Any(key => conditionCheck.ConditionExpression == $"{conditionExpressionFunction}({key})");
        }

        private static string CombinePreLockCondition(
            string conditionExpression,
            ItemRequestDetail itemRequestDetail,
            ItemTransactionState itemTransactionState)
        {
            if (string.IsNullOrWhiteSpace(itemRequestDetail.ConditionExpression))
            {
                return conditionExpression;
            }

            // TODO TODO TODO DODGY ??? This is stopping updates ???
            // if (itemRequestDetail.ConditionExpression.Contains(" AND ") || itemRequestDetail.ConditionExpression.Contains(" OR "))
            // {
            //     throw new NotSupportedException("Condition expressions with AND or OR are not supported");
            // }

            if (itemRequestDetail.Key.Key.Items.Where(item => itemRequestDetail.ConditionExpression.Contains($"attribute_not_exists({item.Key})")).Any())
            {
                // Check the locked item is transient
                if (!itemTransactionState.IsTransient)
                {
                    throw new InvalidOperationException("The request condition is not satisfied");
                }

                // Remove the original condition expression
                return conditionExpression;
            }

            return $"({conditionExpression}) AND ({itemRequestDetail.ConditionExpression})";
        }

        private static AttributeValue Clone(AttributeValue value)
        {
            if (value.S != null)
            {
                return AttributeValueFactory.CreateS(value.S);
            }

            if (value.N != null)
            {
                return AttributeValueFactory.CreateN(value.N);
            }

            if (value.IsBOOLSet)
            {
                return AttributeValueFactory.CreateBOOL(value.BOOL);
            }

            if (value.B != null)
            {
                return AttributeValueFactory.CreateB(
                    new MemoryStream(value.B.ToArray()));
            }

            if (value.BS != null && value.BS.Count > 0)
            {
                return AttributeValueFactory.CreateBS(
                    value.BS.Select(v => new MemoryStream(v.ToArray())).ToList());
            }

            if (value.NS != null && value.NS.Count > 0)
            {
                return AttributeValueFactory.CreateNS(
                    value.NS.Select(v => v).ToList());
            }

            if (value.SS != null && value.SS.Count > 0)
            {
                return AttributeValueFactory.CreateSS(
                    value.SS.Select(v => v).ToList());
            }

            if (value.IsLSet && value.L != null)
            {
                return AttributeValueFactory.CreateL(value.L.Select(Clone).ToList());
            }

            if (value.IsMSet)
            {
                return AttributeValueFactory.CreateM(value.M.ToDictionary(k => k.Key, v => Clone(v.Value)));
            }

            if (value.NULL)
            {
                return AttributeValueFactory.CreateNULL(value.NULL);
            }

            throw new NotSupportedException();
        }

        private static string AddSetStatement(
            string existingUpdateStatement,
            string newSetStatement)
        {
            if (string.IsNullOrWhiteSpace(existingUpdateStatement))
            {
                return $"SET {newSetStatement}";
            }

            if (existingUpdateStatement.Contains("SET"))
            {
                return string.Join(", ", [existingUpdateStatement, newSetStatement]);
            }

            throw new NotSupportedException("Adding to this type of UpdateStatement is not supported");
        }

        private async Task<PutItemResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            PutItemRequest putItemRequest,
            CancellationToken cancellationToken)
        {
            var itemKeys = await requestService.GetItemKeysAsync(putItemRequest, cancellationToken);
            var itemKey = itemKeys.Single();
            var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];
            request.ItemsToBackupByKey.TryGetValue(itemKey, out var itemRecord);
            var item = putItemRequest.Item.ToImmutableDictionary();
            if (itemTransactionState.IsApplied)
            {
                if (putItemRequest.ReturnValues == null || putItemRequest.ReturnValues == ReturnValue.NONE)
                {
                    return new PutItemResponse
                    {
                        Attributes = []
                    };
                }

                if (putItemRequest.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
                {
                    // No old values for transient item
                    return new PutItemResponse
                    {
                        Attributes = []
                    };
                }

                if (putItemRequest.ReturnValues == ReturnValue.ALL_OLD)
                {
                    throw new NotSupportedException("ReturnValue.ALL_OLD is not supported");
                }

                if (putItemRequest.ReturnValues == ReturnValue.ALL_NEW)
                {
                    if (itemRecord == null)
                    {
                        throw new NotSupportedException("ReturnValue.ALL_NEW when ItemRecord is not available in a backup is not supported");
                    }

                    return new PutItemResponse
                    {
                        Attributes = itemRecord
                             .AttributeValues
                             .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                    };
                }

                throw new NotSupportedException();
            }

            // Add the lock id and "is transient" flags to the put request (put replaces) 
            putItemRequest.Item.Add(ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(request.Transaction.Id));
            putItemRequest.Item.Add(ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal));
            if (itemTransactionState.IsTransient)
            {
                putItemRequest.Item.Add(ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal));
            }
            if (itemTransactionState.LastUpdatedDate.HasValue)
            {
                putItemRequest.Item.Add(ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(itemTransactionState.LastUpdatedDate.Value.Ticks.ToString(CultureInfo.InvariantCulture)));
            }
            putItemRequest.ConditionExpression = Combine(
                $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}",
                $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                putItemRequest.ConditionExpression);
            putItemRequest.ExpressionAttributeNames.Add($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value);
            putItemRequest.ExpressionAttributeNames.Add($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value);
            putItemRequest.ExpressionAttributeValues.Add($":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(request.Transaction.Id));
            putItemRequest.ReturnValues = putItemRequest.GetReturnValues() ?? ReturnValue.NONE;

            var putItemResponse = await amazonDynamoDB.PutItemAsync(putItemRequest, cancellationToken);
            if (putItemRequest.ReturnValues == null || putItemRequest.ReturnValues == ReturnValue.NONE)
            {
                return new PutItemResponse
                {
                    Attributes = []
                };
            }

            if (putItemRequest.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
            {
                // No old values for transient item
                return new PutItemResponse
                {
                    Attributes = []
                };
            }

            if (putItemRequest.ReturnValues == ReturnValue.ALL_OLD)
            {
                return new PutItemResponse
                {
                    Attributes = itemRecord?.AttributeValues.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                        ?? []
                };
            }

            if (putItemRequest.ReturnValues == ReturnValue.ALL_NEW)
            {
                var (responseItem, transactionState) = GetItemRecordAndTransactionState(putItemResponse.Attributes);

                return new PutItemResponse
                {
                    Attributes = GetItemResponse(
                        responseItem,
                        itemTransactionState,
                        null,
                        putItemRequest.ExpressionAttributeNames)
                };
            }

            throw new NotSupportedException();
        }

        private static string Combine(params string?[] expressions)
        {
            return string.Join(" AND ", expressions.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e?.Trim()));
        }

        private static DeleteItemResponse ApplyRequest(
            ApplyRequestRequest request,
            DeleteItemRequest deleteItemRequest)
        {
            // No op for the apply part
            var itemKey = ItemKey.Create(deleteItemRequest.TableName, deleteItemRequest.Key.ToImmutableDictionary());
            var returnValues = deleteItemRequest.GetReturnValues() ?? ReturnValue.NONE;
            var transactionState = request.ItemTransactionStatesByKey[itemKey];
            if (returnValues == ReturnValue.ALL_OLD && transactionState.IsTransient)
            {
                return new DeleteItemResponse
                {
                    Attributes = []
                };
            }

            if (returnValues == ReturnValue.ALL_OLD)
            {
                // Deletes are left alone in apply, so return the locked item
                return new DeleteItemResponse
                {
                    Attributes = request.ItemsToBackupByKey[itemKey]
                        .AttributeValues
                        .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                };
            }

            // In the case of NONE or ALL_NEW, it doesn't matter - item is (being) deleted.
            return new DeleteItemResponse
            {
                Attributes = []
            };
        }

        private async Task<GetItemResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            GetItemRequest getItemRequest,
            CancellationToken cancellationToken)
        {
            var itemKey = ItemKey.Create(getItemRequest.TableName, getItemRequest.Key.ToImmutableDictionary());
            var itemTransactionState = request.ItemTransactionStatesByKey[itemKey];
            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Delete)
            {
                // If the item we're getting is deleted in this transaction
                return new GetItemResponse
                {
                    Item = []
                };
            }

            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Get && itemTransactionState.IsTransient)
            {
                // If the item has only a read lock and is transient
                return new GetItemResponse
                {
                    Item = []
                };
            }

            if (request.ItemsToBackupByKey.TryGetValue(itemKey, out var item))
            {
                var projectedAttributeNames = getItemRequest
                    .ProjectionExpression
                    ?.Split(",")
                    .Select(v => v.Trim())
                    .Select(v =>
                    {
                        if (getItemRequest.ExpressionAttributeNames.TryGetValue(v, out var exprName))
                        {
                            return exprName;
                        }

                        return v;
                    })
                    .ToImmutableHashSet();

                return new GetItemResponse
                {
                    Item = item
                        .AttributeValues
                        .Where(kv =>
                            !ItemAttributeName.Values.Contains(new ItemAttributeName(kv.Key)) &&
                            (projectedAttributeNames == null || projectedAttributeNames.Contains(kv.Key)))
                        .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                };
            }

            var response = await amazonDynamoDB.GetItemAsync(getItemRequest, cancellationToken);
            return new GetItemResponse
            {
                Item = response
                    .Item
                    .Where(kv => !ItemAttributeName.Values.Contains(new ItemAttributeName(kv.Key)))
                    .ToDictionary(k => k.Key, v => v.Value)
            };
        }

        private static TransactWriteItem GetAcquireLockRequest(
            ItemTransactionState itemTransactionState,
            bool exists)
        {
            if (string.IsNullOrWhiteSpace(itemTransactionState.TransactionId))
            {
                throw new ArgumentException("TransactionId must be specified", nameof(itemTransactionState));
            }

            if (!itemTransactionState.LastUpdatedDate.HasValue)
            {
                throw new ArgumentException("LastUpdatedDate must be specified", nameof(itemTransactionState));
            }

            var itemExists = $"{string.Join(" AND ", itemTransactionState.Key.Key.Items.Select(k => $"attribute_exists(#{k.Key})"))}";
            var itemNotExists = $"{string.Join(" AND ", itemTransactionState.Key.Key.Items.Select(k => $"attribute_not_exists(#{k.Key})"))}";
            var itemLockedByUs = $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}";
            var itemNotLockedByAny = $"attribute_not_exists(#{ItemAttributeName.TXID.Value})";

            var itemAppliedCondition = itemTransactionState.IsApplied
                ? $"attribute_exists(#{ItemAttributeName.APPLIED.Value})"
                : $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})";

            var itemTransientCondition = itemTransactionState.IsTransient
                ? $"attribute_exists(#{ItemAttributeName.TRANSIENT.Value})"
                : $"attribute_not_exists(#{ItemAttributeName.TRANSIENT.Value})";

            if (exists)
            {
                return new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = itemTransactionState.Key.TableName,
                        Key = itemTransactionState.Key.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                        ConditionExpression = $"({itemExists}) AND {itemAppliedCondition} AND {itemTransientCondition} AND ({itemNotLockedByAny} OR {itemLockedByUs})",
                        UpdateExpression = $"SET #{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}, #{ItemAttributeName.DATE.Value} = :{ItemAttributeName.DATE.Value}",
                        ExpressionAttributeNames = itemTransactionState
                            .Key.Key.Items
                            .Select(k => new KeyValuePair<string, string>($"#{k.Key}", k.Key))
                            .Concat([
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.DATE.Value}", ItemAttributeName.DATE.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                        ])
                            .ToDictionary(k => k.Key, v => v.Value),
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                {$":{ItemAttributeName.TXID.Value}", AttributeValueFactory.CreateS(itemTransactionState.TransactionId)},
                                {$":{ItemAttributeName.DATE.Value}", AttributeValueFactory.CreateN(itemTransactionState.LastUpdatedDate.Value.Ticks.ToString(CultureInfo.InvariantCulture))},
                            },
                        ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD,
                    }
                };
            }

            return new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = itemTransactionState.Key.TableName,
                    Item = itemTransactionState
                        .Key.Key.Items
                        .Select(kv => new KeyValuePair<string, AttributeValue>(kv.Key, kv.Value.ToAttributeValue()))
                        .Concat([
                            new KeyValuePair<string, AttributeValue>(ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS(itemTransactionState.TransactionId)),
                            new KeyValuePair<string, AttributeValue>(ItemAttributeName.DATE.Value, AttributeValueFactory.CreateN(itemTransactionState.LastUpdatedDate.Value.Ticks.ToString(CultureInfo.InvariantCulture))),
                            new KeyValuePair<string, AttributeValue>(ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal)),
                        ])
                        .ToDictionary(k => k.Key, v => v.Value),
                    ConditionExpression = itemNotExists,
                    ExpressionAttributeNames = itemTransactionState
                        .Key.Key.Items
                        .Select(k => new KeyValuePair<string, string>($"#{k.Key}", k.Key))
                        .ToDictionary(k => k.Key, v => v.Value),
                    ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD,
                }
            };
        }

        private static TransactWriteItem? ToReleaseLockTransactWriteItem(
            TransactionId id,
            ItemKey itemKey,
            ItemTransactionState itemTransactionState,
            bool rollback,
            ItemRecord? rollbackImage)
        {
            var isGet = itemTransactionState.LockItemRequestAction.RequestAction is RequestAction.Get or
                        RequestAction.ConditionCheck;

            var isTransientGet = itemTransactionState.IsTransient && isGet;
            var isTransientRollback = itemTransactionState.IsTransient && rollback;
            if (isTransientGet || isTransientRollback)
            {
                return new TransactWriteItem
                {
                    Delete = new Delete
                    {
                        TableName = itemKey.TableName,
                        Key = itemKey.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                        ConditionExpression = $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}_expected AND attribute_exists(#{ItemAttributeName.TRANSIENT.Value})",
                        ExpressionAttributeNames = new Dictionary<string, string> {
                            { $"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value },
                            { $"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                            { $":{ItemAttributeName.TXID.Value}_expected", AttributeValueFactory.CreateS(id.Id)},
                        }
                    }
                };
            }

            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Delete && !rollback && !itemTransactionState.Exists)
            {
                // A delete op which has already been carried out
                return null;
            }

            // further action
            if (itemTransactionState.LockItemRequestAction.RequestAction == RequestAction.Delete && !rollback)
            {
                return new TransactWriteItem
                {
                    Delete = new Delete
                    {
                        TableName = itemKey.TableName,
                        Key = itemKey.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                        ConditionExpression = $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}_expected",
                        ExpressionAttributeNames = new Dictionary<string, string> {
                            { $"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                            { $":{ItemAttributeName.TXID.Value}_expected", AttributeValueFactory.CreateS(id.Id)},
                        }
                    }
                };
            }

            if (rollback && itemTransactionState.TransactionId == null)
            {
                return new TransactWriteItem
                {
                    ConditionCheck = new ConditionCheck
                    {
                        TableName = itemKey.TableName,
                        Key = itemKey.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                        ConditionExpression = $"attribute_not_exists(#{ItemAttributeName.TXID.Value})",
                        ExpressionAttributeNames = new Dictionary<string, string> {
                            { $"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value }
                        },
                    }
                };
            }

            if (rollback && !isGet && itemTransactionState.IsApplied && rollbackImage == null)
            {
                throw new NotSupportedException("Rollback without backup image not supported");
            }

            if (rollback && !isGet && itemTransactionState.IsApplied && rollbackImage != null)
            {
                // If we have 'applied' an update operation then we must use an image to restore it
                return new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = itemKey.TableName,
                        Item = rollbackImage.AttributeValues.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                    }
                };
            }

            if (!rollback && itemTransactionState.LockItemRequestAction.RequestAction != RequestAction.Delete && itemTransactionState.TransactionId == null)
            {
                // A op has already been carried out
                return null;
            }

            return new TransactWriteItem
            {
                Update = new Update
                {
                    TableName = itemKey.TableName,
                    Key = itemKey.Key.Items.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue()),
                    ConditionExpression = $"#{ItemAttributeName.TXID.Value} = :{ItemAttributeName.TXID.Value}_expected",
                    UpdateExpression = $"REMOVE {string.Join(", ", ItemAttributeName.Values.Select(v => $"#{v.Value}"))}",
                    ExpressionAttributeNames = ItemAttributeName.Values.ToDictionary(k => $"#{k.Value}", v => v.Value),
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                            { $":{ItemAttributeName.TXID.Value}_expected", AttributeValueFactory.CreateS(id.Id)},
                        }
                }
            };
        }
    }
}