/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Tests
{
    [TestClass]
    public sealed class RDFTripleKeyWithPartitionTests
    {
        [TestMethod]
        public void CanCompareToWithEqualKeys()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1.CompareTo(key2);

            Assert.AreEqual(0, result);
        }

        [TestMethod]
        public void CanCompareToWithLessThanBySubject()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");

            var result = key1.CompareTo(key2);

            Assert.IsTrue(result < 0);
        }

        [TestMethod]
        public void CanCompareToWithGreaterThanBySubject()
        {
            var key1 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1.CompareTo(key2);

            Assert.IsTrue(result > 0);
        }

        [TestMethod]
        public void CanCompareToWithLessThanByPredicate()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate2", "partition1");

            var result = key1.CompareTo(key2);

            Assert.IsTrue(result < 0);
        }

        [TestMethod]
        public void CanCompareToWithGreaterThanByPredicate()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate2", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1.CompareTo(key2);

            Assert.IsTrue(result > 0);
        }

        [TestMethod]
        public void CanCompareToWithLessThanByPartition()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition2");

            var result = key1.CompareTo(key2);

            Assert.IsTrue(result < 0);
        }

        [TestMethod]
        public void CanCompareToWithGreaterThanByPartition()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition2");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1.CompareTo(key2);

            Assert.IsTrue(result > 0);
        }

        [TestMethod]
        public void CanCompareToWithNull()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1.CompareTo(null);

            Assert.IsTrue(result > 0);
        }

        [TestMethod]
        public void CanUseLessThanOperator()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");

            var result = key1 < key2;

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanUseLessThanOperatorReturnsFalse()
        {
            var key1 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1 < key2;

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanUseGreaterThanOperator()
        {
            var key1 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1 > key2;

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanUseGreaterThanOperatorReturnsFalse()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");

            var result = key1 > key2;

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanUseLessThanOrEqualOperator()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");

            var result = key1 <= key2;

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanUseLessThanOrEqualOperatorWithEqualKeys()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1 <= key2;

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanUseLessThanOrEqualOperatorReturnsFalse()
        {
            var key1 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1 <= key2;

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanUseGreaterThanOrEqualOperator()
        {
            var key1 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1 >= key2;

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanUseGreaterThanOrEqualOperatorWithEqualKeys()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");

            var result = key1 >= key2;

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanUseGreaterThanOrEqualOperatorReturnsFalse()
        {
            var key1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var key2 = new RDFTripleKeyWithPartition("subject2", "predicate1", "partition1");

            var result = key1 >= key2;

            Assert.IsFalse(result);
        }
    }
}
