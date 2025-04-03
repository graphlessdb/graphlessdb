/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class RequestService(
        IAmazonDynamoDBKeyService dynamoDBKeyService) : IRequestService
    {
        public async Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(
            Transaction transaction, CancellationToken cancellationToken)
        {
            var itemRequestGroups = await Task.WhenAll(transaction.Requests.Select(r => GetItemRequestActionsAsync(r, cancellationToken)));

            var existingItemRequestActions = itemRequestGroups
                .SelectMany(r => r)
                .Aggregate(ImmutableDictionary<ItemKey, LockedItemRequestAction>.Empty, (agg, cur) =>
                {
                    if (!agg.TryGetValue(cur.Key, out var value))
                    {
                        return agg.Add(cur.Key, cur);
                    }

                    if (cur.RequestAction == RequestAction.Get)
                    {
                        // Keep the original lock
                        return agg;
                    }

                    if (value.RequestAction == RequestAction.Get)
                    {
                        // Upgrade the lock to write
                        return agg.SetItem(cur.Key, cur);
                    }

                    throw new InvalidOperationException("Previously applied requests are invalid");
                });

            return existingItemRequestActions
                .Values
                .ToImmutableList();
        }

        private async Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(
            RequestRecord requestRecord, CancellationToken cancellationToken)
        {
            var itemRequestActions = await GetItemRequestDetailsAsync(requestRecord.GetRequest(), cancellationToken);
            return itemRequestActions
                .Select(v => new LockedItemRequestAction(v.Key, requestRecord.Id, v.RequestAction))
                .ToImmutableList();
        }

        public async Task<ImmutableList<ItemRequestDetail>> GetItemRequestDetailsAsync(
            AmazonDynamoDBRequest request, CancellationToken cancellationToken)
        {
            if (request is GetItemRequest getItemRequest)
            {
                return [new ItemRequestDetail(
                        ItemKey.Create(getItemRequest.TableName, getItemRequest.Key.ToImmutableDictionary()),
                        RequestAction.Get,
                        null,
                        getItemRequest.ExpressionAttributeNames.ToImmutableDictionary(),
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty)];
            }

            if (request is PutItemRequest putItemRequest)
            {
                var itemKeyAttributes = await dynamoDBKeyService.CreateKeyMapAsync(
                    putItemRequest.TableName,
                    putItemRequest.Item.ToImmutableDictionary(),
                    cancellationToken);

                return [new ItemRequestDetail(
                        ItemKey.Create(putItemRequest.TableName, itemKeyAttributes),
                        RequestAction.Put,
                        putItemRequest.ConditionExpression,
                        putItemRequest.ExpressionAttributeNames.ToImmutableDictionary(),
                        putItemRequest.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value)))];
            }

            if (request is DeleteItemRequest deleteItemRequest)
            {
                return [new ItemRequestDetail(
                        ItemKey.Create(deleteItemRequest.TableName, deleteItemRequest.Key.ToImmutableDictionary()),
                        RequestAction.Delete,
                        deleteItemRequest.ConditionExpression,
                        deleteItemRequest.ExpressionAttributeNames.ToImmutableDictionary(),
                        deleteItemRequest.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value)))];
            }

            if (request is UpdateItemRequest updateItemRequest)
            {
                return [new ItemRequestDetail(
                        ItemKey.Create(updateItemRequest.TableName, updateItemRequest.Key.ToImmutableDictionary()),
                        RequestAction.Update,
                        updateItemRequest.ConditionExpression,
                        updateItemRequest.ExpressionAttributeNames.ToImmutableDictionary(),
                        updateItemRequest.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value)))];
            }

            if (request is TransactGetItemsRequest transactGetItemsRequest)
            {
                var items = transactGetItemsRequest
                    .TransactItems
                    .Select(i => new ItemRequestDetail(
                        ItemKey.Create(i.Get.TableName, i.Get.Key.ToImmutableDictionary()),
                        RequestAction.Get,
                        null,
                        i.Get.ExpressionAttributeNames.ToImmutableDictionary(),
                        ImmutableDictionary<string, ImmutableAttributeValue>.Empty));

                return items.ToImmutableList();
            }

            if (request is TransactWriteItemsRequest transactWriteItemsRequest)
            {
                var items = await Task.WhenAll(transactWriteItemsRequest
                    .TransactItems
                    .Select(async i => new ItemRequestDetail(
                        await GetItemKeyAsync(i, cancellationToken),
                        GetRequestAction(i),
                        GetConditionExpression(i),
                        GetExpressionAttributeNames(i),
                        GetExpressionAttributeValues(i))));

                return [.. items];
            }

            throw new NotSupportedException("Request type is not supported");
        }

        private static RequestAction GetRequestAction(TransactWriteItem item)
        {
            if (item.Put != null)
            {
                return RequestAction.Put;
            }

            if (item.Update != null)
            {
                return RequestAction.Update;
            }

            if (item.Delete != null)
            {
                return RequestAction.Delete;
            }

            if (item.ConditionCheck != null)
            {
                return RequestAction.ConditionCheck;
            }

            throw new NotSupportedException();
        }

        private static string? GetConditionExpression(TransactWriteItem item)
        {
            if (item.Put != null)
            {
                return item.Put.ConditionExpression;
            }

            if (item.Update != null)
            {
                return item.Update.ConditionExpression;
            }

            if (item.Delete != null)
            {
                return item.Delete.ConditionExpression;
            }

            if (item.ConditionCheck != null)
            {
                return item.ConditionCheck.ConditionExpression;
            }

            throw new NotSupportedException();
        }

        private static ImmutableDictionary<string, string> GetExpressionAttributeNames(TransactWriteItem item)
        {
            if (item.Put != null)
            {
                return item.Put.ExpressionAttributeNames.ToImmutableDictionary();
            }

            if (item.Update != null)
            {
                return item.Update.ExpressionAttributeNames.ToImmutableDictionary();
            }

            if (item.Delete != null)
            {
                return item.Delete.ExpressionAttributeNames.ToImmutableDictionary();
            }

            if (item.ConditionCheck != null)
            {
                return item.ConditionCheck.ExpressionAttributeNames.ToImmutableDictionary();
            }

            throw new NotSupportedException();
        }

        private static ImmutableDictionary<string, ImmutableAttributeValue> GetExpressionAttributeValues(TransactWriteItem item)
        {
            if (item.Put != null)
            {
                return item.Put.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value));
            }

            if (item.Update != null)
            {
                return item.Update.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value));
            }

            if (item.Delete != null)
            {
                return item.Delete.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value));
            }

            if (item.ConditionCheck != null)
            {
                return item.ConditionCheck.ExpressionAttributeValues.ToImmutableDictionary(k => k.Key, v => ImmutableAttributeValue.Create(v.Value));
            }

            throw new NotSupportedException();
        }

        private async Task<ItemKey> GetItemKeyAsync(TransactWriteItem item, CancellationToken cancellationToken)
        {
            if (item.Put != null)
            {
                var itemKeyAttributes = await dynamoDBKeyService.CreateKeyMapAsync(
                    item.Put.TableName,
                    item.Put.Item.ToImmutableDictionary(),
                    cancellationToken);

                return ItemKey.Create(item.Put.TableName, itemKeyAttributes);
            }

            if (item.Update != null)
            {
                return ItemKey.Create(item.Update.TableName, item.Update.Key.ToImmutableDictionary());
            }

            if (item.Delete != null)
            {
                return ItemKey.Create(item.Delete.TableName, item.Delete.Key.ToImmutableDictionary());
            }

            if (item.ConditionCheck != null)
            {
                return ItemKey.Create(item.ConditionCheck.TableName, item.ConditionCheck.Key.ToImmutableDictionary());
            }

            throw new NotSupportedException();
        }
    }
}