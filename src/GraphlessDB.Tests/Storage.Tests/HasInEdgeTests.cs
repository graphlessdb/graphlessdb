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
    public sealed class HasInEdgeTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge(null!, "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("", "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("   ", "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph#name", "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", null!, "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "   ", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "node#type", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", null!, "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "   ", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edge#type", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", null!, "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "   ", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "in#1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "in1", null!));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "in1", ""));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "in1", "   "));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdge("graph", "nodeType", "edgeType", "in1", "out#1"));
        }

        #endregion

        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsGraphNameProperty()
        {
            var predicate = new HasInEdge("graph1", "nodeType", "edgeType", "in1", "out1");
            Assert.AreEqual("graph1", predicate.GraphName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInTypeNameProperty()
        {
            var predicate = new HasInEdge("graph1", "nodeType1", "edgeType", "in1", "out1");
            Assert.AreEqual("nodeType1", predicate.NodeInTypeName);
        }

        [TestMethod]
        public void ConstructorSetsEdgeTypeNameProperty()
        {
            var predicate = new HasInEdge("graph1", "nodeType", "edgeType1", "in1", "out1");
            Assert.AreEqual("edgeType1", predicate.EdgeTypeName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInIdProperty()
        {
            var predicate = new HasInEdge("graph1", "nodeType", "edgeType", "in123", "out1");
            Assert.AreEqual("in123", predicate.NodeInId);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutIdProperty()
        {
            var predicate = new HasInEdge("graph1", "nodeType", "edgeType", "in1", "out456");
            Assert.AreEqual("out456", predicate.NodeOutId);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsCorrectFormat()
        {
            var predicate = new HasInEdge("graph1", "nodeType", "edgeType", "in1", "out1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#in#nodeType#edgeType#in1#out1", result);
        }

        [TestMethod]
        public void ToStringHandlesSpecialCharacters()
        {
            var predicate = new HasInEdge("graph1", "nodeType", "edgeType", "in_1", "out_1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#in#nodeType#edgeType#in_1#out_1", result);
        }

        #endregion

        #region IsPredicate Tests

        [TestMethod]
        public void IsPredicateReturnsTrueForValidPredicate()
        {
            var result = HasInEdge.IsPredicate("graph1#in#nodeType#edgeType#in1#out1");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenNotEnoughParts()
        {
            var result = HasInEdge.IsPredicate("graph1#in#nodeType#edgeType#in1");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenTooManyParts()
        {
            var result = HasInEdge.IsPredicate("graph1#in#nodeType#edgeType#in1#out1#extra");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenWrongName()
        {
            var result = HasInEdge.IsPredicate("graph1#out#nodeType#edgeType#in1#out1");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseForEmptyString()
        {
            var result = HasInEdge.IsPredicate("");
            Assert.IsFalse(result);
        }

        #endregion

        #region Parse Tests

        [TestMethod]
        public void ParseCreatesCorrectObject()
        {
            var result = HasInEdge.Parse("graph1#in#nodeType#edgeType#in1#out1");
            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("nodeType", result.NodeInTypeName);
            Assert.AreEqual("edgeType", result.EdgeTypeName);
            Assert.AreEqual("in1", result.NodeInId);
            Assert.AreEqual("out1", result.NodeOutId);
        }

        [TestMethod]
        public void ParseThrowsWhenNotEnoughParts()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasInEdge.Parse("graph1#in#nodeType#edgeType#in1"));
        }

        [TestMethod]
        public void ParseThrowsWhenTooManyParts()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasInEdge.Parse("graph1#in#nodeType#edgeType#in1#out1#extra"));
        }

        [TestMethod]
        public void ParseThrowsWhenWrongName()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasInEdge.Parse("graph1#out#nodeType#edgeType#in1#out1"));
        }

        #endregion

        #region Static Helper Methods Tests

        [TestMethod]
        public void EdgesByTypeNodeInTypeReturnsCorrectFormat()
        {
            var result = HasInEdge.EdgesByTypeNodeInType("graph1", "nodeType");
            Assert.AreEqual("graph1#in#nodeType#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeInTypeAndEdgeTypeReturnsCorrectFormat()
        {
            var result = HasInEdge.EdgesByTypeNodeInTypeAndEdgeType("graph1", "nodeType", "edgeType");
            Assert.AreEqual("graph1#in#nodeType#edgeType#", result);
        }

        #endregion

        #region Name Constant Test

        [TestMethod]
        public void NameConstantHasCorrectValue()
        {
            Assert.AreEqual("in", HasInEdge.Name);
        }

        #endregion
    }
}
