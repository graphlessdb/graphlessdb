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
    public sealed class HasOutEdgeTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge(null!, "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("", "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("   ", "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph#name", "nodeType", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", null!, "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "   ", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "node#type", "edgeType", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", null!, "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "   ", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edge#type", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", null!, "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "   ", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "in#1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "in1", null!));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "in1", ""));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "in1", "   "));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasOutEdge("graph", "nodeType", "edgeType", "in1", "out#1"));
        }

        #endregion

        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsGraphNameProperty()
        {
            var predicate = new HasOutEdge("graph1", "nodeType", "edgeType", "in1", "out1");
            Assert.AreEqual("graph1", predicate.GraphName);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutTypeNameProperty()
        {
            var predicate = new HasOutEdge("graph1", "nodeType1", "edgeType", "in1", "out1");
            Assert.AreEqual("nodeType1", predicate.NodeOutTypeName);
        }

        [TestMethod]
        public void ConstructorSetsEdgeTypeNameProperty()
        {
            var predicate = new HasOutEdge("graph1", "nodeType", "edgeType1", "in1", "out1");
            Assert.AreEqual("edgeType1", predicate.EdgeTypeName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInIdProperty()
        {
            var predicate = new HasOutEdge("graph1", "nodeType", "edgeType", "in123", "out1");
            Assert.AreEqual("in123", predicate.NodeInId);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutIdProperty()
        {
            var predicate = new HasOutEdge("graph1", "nodeType", "edgeType", "in1", "out456");
            Assert.AreEqual("out456", predicate.NodeOutId);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsCorrectFormat()
        {
            var predicate = new HasOutEdge("graph1", "nodeType", "edgeType", "in1", "out1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#out#nodeType#edgeType#in1#out1", result);
        }

        [TestMethod]
        public void ToStringHandlesSpecialCharacters()
        {
            var predicate = new HasOutEdge("graph1", "nodeType", "edgeType", "in_1", "out_1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#out#nodeType#edgeType#in_1#out_1", result);
        }

        #endregion

        #region IsPredicate Tests

        [TestMethod]
        public void IsPredicateReturnsTrueForValidPredicate()
        {
            var result = HasOutEdge.IsPredicate("graph1#out#nodeType#edgeType#in1#out1");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenNotEnoughParts()
        {
            var result = HasOutEdge.IsPredicate("graph1#out#nodeType#edgeType#in1");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenTooManyParts()
        {
            var result = HasOutEdge.IsPredicate("graph1#out#nodeType#edgeType#in1#out1#extra");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenWrongName()
        {
            var result = HasOutEdge.IsPredicate("graph1#in#nodeType#edgeType#in1#out1");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseForEmptyString()
        {
            var result = HasOutEdge.IsPredicate("");
            Assert.IsFalse(result);
        }

        #endregion

        #region Parse Tests

        [TestMethod]
        public void ParseCreatesCorrectObject()
        {
            var result = HasOutEdge.Parse("graph1#out#nodeType#edgeType#in1#out1");
            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("nodeType", result.NodeOutTypeName);
            Assert.AreEqual("edgeType", result.EdgeTypeName);
            Assert.AreEqual("in1", result.NodeInId);
            Assert.AreEqual("out1", result.NodeOutId);
        }

        [TestMethod]
        public void ParseThrowsWhenNotEnoughParts()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasOutEdge.Parse("graph1#out#nodeType#edgeType#in1"));
        }

        [TestMethod]
        public void ParseThrowsWhenTooManyParts()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasOutEdge.Parse("graph1#out#nodeType#edgeType#in1#out1#extra"));
        }

        [TestMethod]
        public void ParseThrowsWhenWrongName()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasOutEdge.Parse("graph1#in#nodeType#edgeType#in1#out1"));
        }

        #endregion

        #region Static Helper Methods Tests

        [TestMethod]
        public void EdgesByTypeNodeOutTypeReturnsCorrectFormat()
        {
            var result = HasOutEdge.EdgesByTypeNodeOutType("graph1", "nodeType");
            Assert.AreEqual("graph1#out#nodeType#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeOutTypeAndEdgeTypeReturnsCorrectFormat()
        {
            var result = HasOutEdge.EdgesByTypeNodeOutTypeAndEdgeType("graph1", "nodeType", "edgeType");
            Assert.AreEqual("graph1#out#nodeType#edgeType#", result);
        }

        #endregion

        #region Name Constant Test

        [TestMethod]
        public void NameConstantHasCorrectValue()
        {
            Assert.AreEqual("out", HasOutEdge.Name);
        }

        #endregion
    }
}
