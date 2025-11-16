/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class QueryRDFTriplesByPartitionAndIndexedObjectRequestTests
    {
        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsTableNameProperty()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.AreEqual("TestTable", request.TableName);
        }

        [TestMethod]
        public void ConstructorSetsPartitionProperty()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.AreEqual("partition1", request.Partition);
        }

        [TestMethod]
        public void ConstructorSetsIndexedObjectBeginsWithProperty()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.AreEqual("object#", request.IndexedObjectBeginsWith);
        }

        [TestMethod]
        public void ConstructorSetsExclusiveStartKeyPropertyToNull()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsNull(request.ExclusiveStartKey);
        }

        [TestMethod]
        public void ConstructorSetsExclusiveStartKeyPropertyToValue()
        {
            var startKey = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                startKey,
                true,
                10,
                false);
            Assert.AreEqual(startKey, request.ExclusiveStartKey);
        }

        [TestMethod]
        public void ConstructorSetsScanIndexForwardPropertyToTrue()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsTrue(request.ScanIndexForward);
        }

        [TestMethod]
        public void ConstructorSetsScanIndexForwardPropertyToFalse()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                false,
                10,
                false);
            Assert.IsFalse(request.ScanIndexForward);
        }

        [TestMethod]
        public void ConstructorSetsLimitProperty()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                25,
                false);
            Assert.AreEqual(25, request.Limit);
        }

        [TestMethod]
        public void ConstructorSetsConsistentReadPropertyToTrue()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                true);
            Assert.IsTrue(request.ConsistentRead);
        }

        [TestMethod]
        public void ConstructorSetsConsistentReadPropertyToFalse()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request.ConsistentRead);
        }

        #endregion

        #region Equality Tests

        [TestMethod]
        public void EqualsReturnsTrueWhenAllPropertiesMatch()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsTrue(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenTableNameDiffers()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable1",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable2",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenPartitionDiffers()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition2",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenIndexedObjectBeginsWithDiffers()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object1#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object2#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenExclusiveStartKeyDiffers()
        {
            var startKey1 = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var startKey2 = new RDFTripleKeyWithPartition("subject2", "predicate2", "partition2");
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                startKey1,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                startKey2,
                true,
                10,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenScanIndexForwardDiffers()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                false,
                10,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenLimitDiffers()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                20,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenConsistentReadDiffers()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                true);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request1.Equals(request2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithNull()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request.Equals(null));
        }

        [TestMethod]
        public void EqualsReturnsTrueWhenComparedWithSameInstance()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsTrue(request.Equals(request));
        }

        #endregion

        #region GetHashCode Tests

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualObjects()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.AreEqual(request1.GetHashCode(), request2.GetHashCode());
        }

        [TestMethod]
        public void GetHashCodeReturnsDifferentValueForDifferentObjects()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable1",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable2",
                "partition2",
                "object2#",
                null,
                false,
                20,
                true);
            Assert.AreNotEqual(request1.GetHashCode(), request2.GetHashCode());
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsFormattedString()
        {
            var request = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var result = request.ToString();
            Assert.IsTrue(result.Contains("TestTable"));
            Assert.IsTrue(result.Contains("partition1"));
            Assert.IsTrue(result.Contains("object#"));
        }

        #endregion

        #region Operator == Tests

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenEqual()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsTrue(request1 == request2);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenNotEqual()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable1",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable2",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request1 == request2);
        }

        #endregion

        #region Operator != Tests

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenEqual()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsFalse(request1 != request2);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenNotEqual()
        {
            var request1 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable1",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var request2 = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable2",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            Assert.IsTrue(request1 != request2);
        }

        #endregion

        #region With Expression Tests

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedTableName()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var modified = original with { TableName = "NewTable" };
            Assert.AreEqual("NewTable", modified.TableName);
            Assert.AreEqual("partition1", modified.Partition);
            Assert.AreEqual("object#", modified.IndexedObjectBeginsWith);
        }

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedPartition()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var modified = original with { Partition = "partition2" };
            Assert.AreEqual("TestTable", modified.TableName);
            Assert.AreEqual("partition2", modified.Partition);
        }

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedIndexedObjectBeginsWith()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var modified = original with { IndexedObjectBeginsWith = "newObject#" };
            Assert.AreEqual("newObject#", modified.IndexedObjectBeginsWith);
        }

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedExclusiveStartKey()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var newStartKey = new RDFTripleKeyWithPartition("subject1", "predicate1", "partition1");
            var modified = original with { ExclusiveStartKey = newStartKey };
            Assert.AreEqual(newStartKey, modified.ExclusiveStartKey);
        }

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedScanIndexForward()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var modified = original with { ScanIndexForward = false };
            Assert.IsFalse(modified.ScanIndexForward);
        }

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedLimit()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var modified = original with { Limit = 50 };
            Assert.AreEqual(50, modified.Limit);
        }

        [TestMethod]
        public void WithExpressionCreatesNewInstanceWithModifiedConsistentRead()
        {
            var original = new QueryRDFTriplesByPartitionAndIndexedObjectRequest(
                "TestTable",
                "partition1",
                "object#",
                null,
                true,
                10,
                false);
            var modified = original with { ConsistentRead = true };
            Assert.IsTrue(modified.ConsistentRead);
        }

        #endregion
    }
}
