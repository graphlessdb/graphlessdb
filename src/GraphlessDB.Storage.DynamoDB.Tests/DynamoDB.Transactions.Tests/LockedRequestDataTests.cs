/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Collections.Immutable;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class LockedRequestDataTests
    {
        [TestMethod]
        public void ConstructorInitializesAllProperties()
        {
            var itemKey1 = new ItemKey("Table1", new ImmutableDictionarySequence<string, ImmutableAttributeValue>(ImmutableDictionary<string, ImmutableAttributeValue>.Empty));
            var itemKey2 = new ItemKey("Table2", new ImmutableDictionarySequence<string, ImmutableAttributeValue>(ImmutableDictionary<string, ImmutableAttributeValue>.Empty));
            var itemKeys = ImmutableList.Create(itemKey1, itemKey2);
            
            var itemRecord = new ItemRecord(itemKey1, ImmutableDictionary<string, ImmutableAttributeValue>.Empty);
            var itemsByKey = ImmutableDictionary<ItemKey, ItemRecord>.Empty.Add(itemKey1, itemRecord);
            
            var lockedAction = new LockedItemRequestAction(itemKey1, 1, RequestAction.Get);
            var transactionState = new ItemTransactionState(itemKey1, true, "txn1", null, false, false, lockedAction);
            var itemTransactionStatesByKey = ImmutableDictionary<ItemKey, ItemTransactionState>.Empty.Add(itemKey1, transactionState);
            
            var itemRequestActionsByKey = ImmutableDictionary<ItemKey, LockedItemRequestAction>.Empty.Add(itemKey1, lockedAction);

            var lockedRequestData = new LockedRequestData(
                itemKeys,
                itemsByKey,
                itemTransactionStatesByKey,
                itemRequestActionsByKey);

            Assert.AreSame(itemKeys, lockedRequestData.ItemKeys);
            Assert.AreSame(itemsByKey, lockedRequestData.ItemsByKey);
            Assert.AreSame(itemTransactionStatesByKey, lockedRequestData.ItemTransactionStatesByKey);
            Assert.AreSame(itemRequestActionsByKey, lockedRequestData.ItemRequestActionsByKey);
        }

        [TestMethod]
        public void RecordEqualityWorksCorrectly()
        {
            var itemKeys = ImmutableList<ItemKey>.Empty;
            var itemsByKey = ImmutableDictionary<ItemKey, ItemRecord>.Empty;
            var itemTransactionStatesByKey = ImmutableDictionary<ItemKey, ItemTransactionState>.Empty;
            var itemRequestActionsByKey = ImmutableDictionary<ItemKey, LockedItemRequestAction>.Empty;

            var data1 = new LockedRequestData(
                itemKeys,
                itemsByKey,
                itemTransactionStatesByKey,
                itemRequestActionsByKey);

            var data2 = new LockedRequestData(
                itemKeys,
                itemsByKey,
                itemTransactionStatesByKey,
                itemRequestActionsByKey);

            Assert.AreEqual(data1, data2);
            Assert.IsTrue(data1 == data2);
            Assert.IsFalse(data1 != data2);
        }
    }
}
