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
    public sealed class HadOutEdgeTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsNull()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge(null!, "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsEmpty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsWhitespace()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("   ", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameContainsHash()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph#name", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameIsNull()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", null!, "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameIsEmpty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameIsWhitespace()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "   ", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameContainsHash()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "node#type", "edgeType", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsNull()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", null!, "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsEmpty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsWhitespace()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "   ", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameContainsHash()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edge#type", "in1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsNull()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", null!, "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsEmpty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsWhitespace()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "   ", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdContainsHash()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "in#1", "out1", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsNull()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "in1", null!, createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsEmpty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "in1", "", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsWhitespace()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "in1", "   ", createdAt, deletedAt));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdContainsHash()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            Assert.ThrowsException<ArgumentException>(() =>
                new HadOutEdge("graph", "nodeType", "edgeType", "in1", "out#1", createdAt, deletedAt));
        }

        #endregion

        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsGraphNameProperty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual("graph1", predicate.GraphName);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutTypeNameProperty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            var predicate = new HadOutEdge("graph1", "nodeType1", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual("nodeType1", predicate.NodeOutTypeName);
        }

        [TestMethod]
        public void ConstructorSetsEdgeTypeNameProperty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType1", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual("edgeType1", predicate.EdgeTypeName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInIdProperty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in123", "out1", createdAt, deletedAt);
            Assert.AreEqual("in123", predicate.NodeInId);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutIdProperty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = DateTime.UtcNow.AddDays(1);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in1", "out456", createdAt, deletedAt);
            Assert.AreEqual("out456", predicate.NodeOutId);
        }

        [TestMethod]
        public void ConstructorSetsCreatedAtProperty()
        {
            var createdAt = new DateTime(2023, 5, 15, 10, 30, 45, DateTimeKind.Utc);
            var deletedAt = DateTime.UtcNow.AddDays(1);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual(createdAt, predicate.CreatedAt);
        }

        [TestMethod]
        public void ConstructorSetsDeletedAtProperty()
        {
            var createdAt = DateTime.UtcNow;
            var deletedAt = new DateTime(2023, 6, 20, 15, 45, 30, DateTimeKind.Utc);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            Assert.AreEqual(deletedAt, predicate.DeletedAt);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsCorrectFormat()
        {
            var createdAt = new DateTime(2023, 5, 15, 10, 30, 45, DateTimeKind.Utc);
            var deletedAt = new DateTime(2023, 6, 20, 15, 45, 30, DateTimeKind.Utc);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in1", "out1", createdAt, deletedAt);
            var result = predicate.ToString();
            Assert.AreEqual("graph1#hadOut#nodeType#edgeType#in1#out1#2023-05-15 10:30:45Z#2023-06-20 15:45:30Z", result);
        }

        [TestMethod]
        public void ToStringHandlesSpecialCharacters()
        {
            var createdAt = new DateTime(2023, 5, 15, 10, 30, 45, DateTimeKind.Utc);
            var deletedAt = new DateTime(2023, 6, 20, 15, 45, 30, DateTimeKind.Utc);
            var predicate = new HadOutEdge("graph1", "nodeType", "edgeType", "in_1", "out_1", createdAt, deletedAt);
            var result = predicate.ToString();
            Assert.AreEqual("graph1#hadOut#nodeType#edgeType#in_1#out_1#2023-05-15 10:30:45Z#2023-06-20 15:45:30Z", result);
        }

        #endregion

        #region Name Constant Test

        [TestMethod]
        public void NameConstantHasCorrectValue()
        {
            Assert.AreEqual("hadOut", HadOutEdge.Name);
        }

        #endregion
    }
}
