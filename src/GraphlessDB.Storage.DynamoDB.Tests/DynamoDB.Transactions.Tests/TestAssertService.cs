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
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    public sealed class TestAssertService(
        ITransactionStore transactionStore,
        IAmazonDynamoDBWithTransactions transactionService,
        IItemImageStore itemImageStore,
        IRequestService requestService,
        IAmazonDynamoDB dynamoDB)
    {
        public async Task AssertItemLockedAsync(
           string tableName,
           ImmutableDictionary<string, AttributeValue> key,
           ImmutableDictionary<string, AttributeValue>? expected,
           string owner, bool isTransient, bool isApplied,
           CancellationToken cancellationToken)
        {
            await AssertItemLockedAsync(tableName, key, expected, owner, isTransient, isApplied, true, cancellationToken);
        }

        public async Task AssertItemLockedAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> key,
            ImmutableDictionary<string, AttributeValue>? expected,
            string owner, bool isTransient, bool isApplied, bool checkTransaction,
            CancellationToken cancellationToken)
        {
            var item = await GetItemAsync(tableName, key, cancellationToken);
            Assert.IsNotNull(item);
            Assert.AreEqual(owner, item[ItemAttributeName.TXID.Value].S);
            if (isTransient)
            {
                Assert.IsTrue(item.ContainsKey(ItemAttributeName.TRANSIENT.Value), "item is not transient, and should have been");
                Assert.AreEqual("1", item[ItemAttributeName.TRANSIENT.Value].S, "item is not transient, and should have been");
            }
            else
            {
                Assert.IsFalse(item.ContainsKey(ItemAttributeName.TRANSIENT.Value), "item is transient, and should not have been");
            }
            if (isApplied)
            {
                Assert.IsTrue(item.ContainsKey(ItemAttributeName.APPLIED.Value), "item is not applied, and should have been");
                Assert.AreEqual("1", item[ItemAttributeName.APPLIED.Value].S, "item is not applied, and should have been");
            }
            else
            {
                Assert.IsFalse(item.ContainsKey(ItemAttributeName.APPLIED.Value), "item is applied, and should not have been");
            }

            Assert.IsTrue(item.ContainsKey(ItemAttributeName.DATE.Value));
            if (expected != null)
            {
                var cleanedItem = item
                    .Remove(ItemAttributeName.TXID.Value)
                    .Remove(ItemAttributeName.TRANSIENT.Value)
                    .Remove(ItemAttributeName.APPLIED.Value)
                    .Remove(ItemAttributeName.DATE.Value);
                Assert.IsTrue(TestHelpers.AreEqual(expected, cleanedItem));
            }

            // Also verify that it is locked in the transaction record
            if (checkTransaction)
            {
                var transaction = await transactionStore.GetAsync(new TransactionId(owner), true, cancellationToken);
                var itemRequestActions = await requestService.GetItemRequestActionsAsync(transaction, cancellationToken);
                var itemKey = ItemKey.Create(tableName, key);
                var lockingRequestAction = itemRequestActions.Where(a => a.Key == itemKey).FirstOrDefault();
                Assert.IsTrue(lockingRequestAction != null);
            }
        }

        public async Task AssertItemLockedAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> key,
            string owner, bool isTransient, bool isApplied,
            CancellationToken cancellationToken)
        {
            await AssertItemLockedAsync(tableName, key, null, owner, isTransient, isApplied, cancellationToken);
        }

        public async Task AssertItemNotLockedAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> key,
            ImmutableDictionary<string, AttributeValue>? expected,
            bool shouldExist,
            CancellationToken cancellationToken)
        {
            var item = await GetItemAsync(tableName, key, cancellationToken);
            if (shouldExist)
            {
                Assert.IsTrue(!item.IsEmpty, "Item does not exist in the table, but it should");
                Assert.IsFalse(item.ContainsKey(ItemAttributeName.TRANSIENT.Value));
                Assert.IsFalse(item.ContainsKey(ItemAttributeName.TXID.Value));
                Assert.IsFalse(item.ContainsKey(ItemAttributeName.APPLIED.Value));
                Assert.IsFalse(item.ContainsKey(ItemAttributeName.DATE.Value));
            }
            else
            {
                Assert.IsTrue(item.IsEmpty);
            }

            if (expected != null)
            {
                var cleanedItem = item
                    .Remove(ItemAttributeName.TXID.Value)
                    .Remove(ItemAttributeName.TRANSIENT.Value);
                Assert.IsTrue(TestHelpers.AreEqual(expected, cleanedItem));
            }
        }

        public async Task AssertItemNotLockedAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> key,
            bool shouldExist,
            CancellationToken cancellationToken)
        {
            await AssertItemNotLockedAsync(tableName, key, null, shouldExist, cancellationToken);
        }

        public void AssertNoSpecialAttributes(ImmutableDictionary<string, AttributeValue> item)
        {
            var attr = ItemAttributeName.Values.FirstOrDefault(v => item.ContainsKey(v.Value));
            if (attr != null)
            {
                Assert.Fail("Should not have contained attribute " + attr.Value);
            }
        }

        public async Task AssertOldItemImageAsync(
            string transactionId,
            string tableName,
            ImmutableDictionary<string, AttributeValue> key,
            ImmutableDictionary<string, AttributeValue>? item,
            bool shouldExist,
            CancellationToken cancellationToken)
        {
            var transaction = await transactionStore.GetAsync(new TransactionId(transactionId), true, cancellationToken);
            var itemRequestActions = await requestService.GetItemRequestActionsAsync(transaction, cancellationToken);
            var itemKey = ItemKey.Create(tableName, key);
            var lockingRequestAction = itemRequestActions
                .Where(a => a.Key == itemKey)
                .FirstOrDefault() ?? throw new InvalidOperationException("Expected request");

            var imageItems = await itemImageStore.GetItemImagesAsync(
                new TransactionVersion(transaction.Id, lockingRequestAction.RequestId),
                cancellationToken);

            if (shouldExist)
            {
                var image = imageItems.Single();
                Assert.IsTrue(!image.AttributeValues.IsEmpty);
                Assert.IsTrue(image.AttributeValues.ContainsKey(ItemAttributeName.TXID.Value));
                Assert.IsTrue(image.AttributeValues.ContainsKey(ImageAttributeName.ImageId.Value));
                Assert.IsTrue(image.AttributeValues.ContainsKey(ItemAttributeName.DATE.Value));
                Assert.IsFalse(image.AttributeValues.ContainsKey(ItemAttributeName.TRANSIENT.Value));

                // NOTE does not work for Set AttributeValue types (DynamoDB does not preserve ordering)
                Assert.IsTrue(TestHelpers.AreEqual(item ?? ImmutableDictionary<string, AttributeValue>.Empty, image.AttributeValues.ToImmutableDictionary(k => k.Key, v => v.Value.ToAttributeValue())));
            }
            else
            {
                Assert.IsTrue(imageItems.IsEmpty);
            }
        }

        public async Task AssertTransactionDeletedAsync(
            TransactionId t,
            CancellationToken cancellationToken)
        {
            try
            {
                await transactionService.ResumeTransactionAsync(t, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionNotFoundException)
            {
            }
        }

        private async Task<ImmutableDictionary<string, AttributeValue>> GetItemAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> key,
            CancellationToken cancellationToken)
        {
            var result = await dynamoDB
                .GetItemAsync(new GetItemRequest
                {
                    TableName = tableName,
                    Key = key.ToDictionary(k => k.Key, v => v.Value),
                    ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                    ConsistentRead = true
                }, cancellationToken);

            return result.Item.ToImmutableDictionary();
        }
    }
}
