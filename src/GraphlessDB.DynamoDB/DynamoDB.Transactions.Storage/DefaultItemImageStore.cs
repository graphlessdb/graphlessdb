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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Internal;
using Microsoft.Extensions.Options;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class DefaultItemImageStore(
        IOptionsSnapshot<ItemImageStoreOptions> options,
        ITransactionServiceEvents transactionServiceEvents,
        IAmazonDynamoDB amazonDynamoDB) : IItemImageStore
    {
        public Task<ImmutableList<TransactionVersion>> GetTransactionVersions(
            TransactionId id, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public async Task<ImmutableList<ItemRecord>> GetItemImagesAsync(
            TransactionVersion transactionVersion,
            CancellationToken cancellationToken)
        {
            TransactionAssertionException.TxAssert(transactionVersion.Version > 0, transactionVersion.Id, "Expected rid > 0");

            var getItemRequest = new GetItemRequest
            {
                ConsistentRead = true,
                TableName = options.Value.ItemImageTableName,
                Key = GetKey(transactionVersion)
            };

            var itemResponse = await amazonDynamoDB.GetItemAsync(getItemRequest, cancellationToken);
            if (!itemResponse.IsItemSet)
            {
                return [];
            }

            var json = itemResponse.Item[ImageAttributeName.ImageValue.Value].S;
            return JsonSerializer.Deserialize(json, ItemImageStoreSerializerContext.Default.ImmutableListItemRecord)
                ?? throw new InvalidOperationException("Failed to retrieve item images");
        }

        public Dictionary<string, AttributeValue> GetKey(TransactionVersion transactionVersion)
        {
            return new Dictionary<string, AttributeValue>{
                { ImageAttributeName.ImageId.Value, AttributeValueFactory.CreateS(GetImageIdValue(transactionVersion))}
            };
        }

        public Task BackupItemImagesAsync(
            TransactionId id,
            AmazonDynamoDBRequest request,
            int requestId,
            CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public async Task AddItemImagesAsync(
            TransactionVersion transactionVersion,
            ImmutableList<ItemRecord> items,
            CancellationToken cancellationToken)
        {
            if (transactionServiceEvents.OnBackupItemImagesAsync != null)
            {
                await transactionServiceEvents.OnBackupItemImagesAsync(transactionVersion.GetId(), cancellationToken);
            }

            if (items.IsEmpty)
            {
                return;
            }

            var json = JsonSerializer.Serialize(items, ItemImageStoreSerializerContext.Default.ImmutableListItemRecord);

            var putItemRequest = new PutItemRequest
            {
                TableName = options.Value.ItemImageTableName,
                ConditionExpression = $"attribute_not_exists(#{ImageAttributeName.ImageId.Value})",
                Item = new Dictionary<string, AttributeValue> {
                    {ImageAttributeName.ImageId.Value, AttributeValueFactory.CreateS(GetImageIdValue(transactionVersion))},
                    {ImageAttributeName.ImageValue.Value, AttributeValueFactory.CreateS(json)},
                },
                ExpressionAttributeNames = new Dictionary<string, string> {
                    { $"#{ImageAttributeName.ImageId.Value}", ImageAttributeName.ImageId.Value}
                }
            };

            await amazonDynamoDB.PutItemAsync(putItemRequest, cancellationToken);
        }

        public async Task DeleteItemImagesAsync(
            TransactionVersion transactionVersion,
            CancellationToken cancellationToken)
        {
            var deleteItemRequest = new DeleteItemRequest
            {
                TableName = options.Value.ItemImageTableName,
                Key = new Dictionary<string, AttributeValue> {
                    {ImageAttributeName.ImageId.Value, AttributeValueFactory.CreateS(GetImageIdValue(transactionVersion))}
                }
            };

            await amazonDynamoDB.DeleteItemAsync(deleteItemRequest, cancellationToken);
        }

        private static string GetImageIdValue(TransactionVersion transactionVersion)
        {
            return $"{transactionVersion.Id}_{transactionVersion.Version}";
        }
    }
}
