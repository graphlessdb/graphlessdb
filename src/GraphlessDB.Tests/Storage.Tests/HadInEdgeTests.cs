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

namespace GraphlessDB.Tests.Storage
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class HadInEdgeTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsNull()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge(null!, "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsEmpty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsWhitespace()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("   ", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameContainsHash()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph#name", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsNull()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", null!, "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsEmpty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsWhitespace()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "   ", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameContainsHash()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "node#type", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsNull()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", null!, "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsEmpty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsWhitespace()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "   ", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameContainsHash()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edge#type", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsNull()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", null!, "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsEmpty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsWhitespace()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "   ", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdContainsHash()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "in#1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsNull()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "in1", null!, createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsEmpty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "in1", "", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsWhitespace()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "in1", "   ", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdContainsHash()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadInEdge("graph", "nodeType", "edgeType", "in1", "out#1", createdAt, deletedAt));
        }

        #endregion

        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsGraphNameProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual("graph1", predicate.GraphName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInTypeNameProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType1", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual("nodeType1", predicate.NodeInTypeName);
        }

        [TestMethod]
        public void ConstructorSetsEdgeTypeNameProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType1", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual("edgeType1", predicate.EdgeTypeName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInIdProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in123", "out1", createdAt, deletedAt);
            Assert.AreEqual("in123", predicate.NodeInId);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutIdProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in1", "out456", createdAt, deletedAt);
            Assert.AreEqual("out456", predicate.NodeOutId);
        }

        [TestMethod]
        public void ConstructorSetsCreatedAtProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 12, 30, 45, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual(createdAt, predicate.CreatedAt);
        }

        [TestMethod]
        public void ConstructorSetsDeletedAtProperty()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 14, 25, 30, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual(deletedAt, predicate.DeletedAt);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsCorrectFormat()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            var result = predicate.ToString();
            Assert.AreEqual("graph1#hadIn#nodeType#edgeType#in1#out1#2024-01-01 00:00:00Z#2024-01-02 00:00:00Z", result);
        }

        [TestMethod]
        public void ToStringHandlesSpecialCharacters()
        {
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in_1", "out_1", createdAt, deletedAt);
            var result = predicate.ToString();
            Assert.AreEqual("graph1#hadIn#nodeType#edgeType#in_1#out_1#2024-01-01 00:00:00Z#2024-01-02 00:00:00Z", result);
        }

        [TestMethod]
        public void ToStringHandlesDifferentDateTimes()
        {
            var createdAt = new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 12, 25, 23, 59, 59, DateTimeKind.Utc);
            var predicate = new HadInEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            var result = predicate.ToString();
            Assert.AreEqual("graph1#hadIn#nodeType#edgeType#in1#out1#2024-06-15 10:30:45Z#2024-12-25 23:59:59Z", result);
        }

        #endregion

        #region Name Constant Test

        [TestMethod]
        public void NameConstantHasCorrectValue()
        {
            Assert.AreEqual("hadIn", HadInEdge.Name);
        }

        #endregion
    }
}
