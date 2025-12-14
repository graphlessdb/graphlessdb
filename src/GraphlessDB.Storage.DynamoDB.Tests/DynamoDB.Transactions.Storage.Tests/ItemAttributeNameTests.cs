/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Storage.Tests
{
    [TestClass]
    public sealed class ItemAttributeNameTests
    {
        [TestMethod]
        public void ConstructorInitializesValue()
        {
            var attributeName = new ItemAttributeName("TestValue");

            Assert.AreEqual("TestValue", attributeName.Value);
        }

        [TestMethod]
        public void ToStringReturnsValue()
        {
            var attributeName = new ItemAttributeName("TestAttribute");

            var result = attributeName.ToString();

            Assert.AreEqual("TestAttribute", result);
        }

        [TestMethod]
        public void TXIDHasCorrectValue()
        {
            Assert.AreEqual("_TxId", ItemAttributeName.TXID.Value);
        }

        [TestMethod]
        public void DATEHasCorrectValue()
        {
            Assert.AreEqual("_TxD", ItemAttributeName.DATE.Value);
        }

        [TestMethod]
        public void TRANSIENTHasCorrectValue()
        {
            Assert.AreEqual("_TxT", ItemAttributeName.TRANSIENT.Value);
        }

        [TestMethod]
        public void APPLIEDHasCorrectValue()
        {
            Assert.AreEqual("_TxA", ItemAttributeName.APPLIED.Value);
        }

        [TestMethod]
        public void ValuesContainsAllStaticFields()
        {
            Assert.IsTrue(ItemAttributeName.Values.Contains(ItemAttributeName.TXID));
            Assert.IsTrue(ItemAttributeName.Values.Contains(ItemAttributeName.DATE));
            Assert.IsTrue(ItemAttributeName.Values.Contains(ItemAttributeName.TRANSIENT));
            Assert.IsTrue(ItemAttributeName.Values.Contains(ItemAttributeName.APPLIED));
        }

        [TestMethod]
        public void ValuesHasCorrectCount()
        {
            Assert.AreEqual(4, ItemAttributeName.Values.Count);
        }

        [TestMethod]
        public void RecordEqualityWorksCorrectly()
        {
            var name1 = new ItemAttributeName("Test");
            var name2 = new ItemAttributeName("Test");
            var name3 = new ItemAttributeName("Different");

            Assert.AreEqual(name1, name2);
            Assert.AreNotEqual(name1, name3);
        }

        [TestMethod]
        public void RecordHashCodeWorksCorrectly()
        {
            var name1 = new ItemAttributeName("Test");
            var name2 = new ItemAttributeName("Test");

            Assert.AreEqual(name1.GetHashCode(), name2.GetHashCode());
        }
    }
}
