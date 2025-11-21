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

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class HasInEdgePropTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp(null!, "nodeType", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("", "nodeType", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("   ", "nodeType", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph#name", "nodeType", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", null!, "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "   ", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "node#type", "edgeType", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", null!, "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "   ", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEdgeTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edge#type", "prop", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", null!, "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "   ", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop#name", "value", "in1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", null!, "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "   ", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeInIdContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "in#1", "out1"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "in1", null!));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "in1", ""));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "in1", "   "));
        }

        [TestMethod]
        public void ConstructorThrowsWhenNodeOutIdContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasInEdgeProp("graph", "nodeType", "edgeType", "prop", "value", "in1", "out#1"));
        }

        #endregion

        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsGraphNameProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", "value", "in1", "out1");
            Assert.AreEqual("graph1", predicate.GraphName);
        }

        [TestMethod]
        public void ConstructorSetsNodeInTypeNameProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType1", "edgeType", "prop", "value", "in1", "out1");
            Assert.AreEqual("nodeType1", predicate.NodeInTypeName);
        }

        [TestMethod]
        public void ConstructorSetsEdgeTypeNameProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType1", "prop", "value", "in1", "out1");
            Assert.AreEqual("edgeType1", predicate.EdgeTypeName);
        }

        [TestMethod]
        public void ConstructorSetsPropertyNameProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop1", "value", "in1", "out1");
            Assert.AreEqual("prop1", predicate.PropertyName);
        }

        [TestMethod]
        public void ConstructorSetsPropertyValueProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", "value1", "in1", "out1");
            Assert.AreEqual("value1", predicate.PropertyValue);
        }

        [TestMethod]
        public void ConstructorSetsPropertyValueToNullWhenNull()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", null!, "in1", "out1");
            Assert.IsNull(predicate.PropertyValue);
        }

        [TestMethod]
        public void ConstructorSetsNodeInIdProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", "value", "in123", "out1");
            Assert.AreEqual("in123", predicate.NodeInId);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutIdProperty()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", "value", "in1", "out456");
            Assert.AreEqual("out456", predicate.NodeOutId);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsCorrectFormat()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", "value", "in1", "out1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#prop#value#in1#out1", result);
        }

        [TestMethod]
        public void ToStringHandlesPropertyValueWithHash()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", "value#with#hash", "in1", "out1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#prop#value#with#hash#in1#out1", result);
        }

        [TestMethod]
        public void ToStringHandlesNullPropertyValue()
        {
            var predicate = new HasInEdgeProp("graph1", "nodeType", "edgeType", "prop", null!, "in1", "out1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#prop##in1#out1", result);
        }

        #endregion

        #region IsPredicate Tests

        [TestMethod]
        public void IsPredicateReturnsTrueForValidPredicate()
        {
            var result = HasInEdgeProp.IsPredicate("graph1#inProp#nodeType#edgeType#prop#value#in1#out1");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsTrueForPredicateWithHashInValue()
        {
            var result = HasInEdgeProp.IsPredicate("graph1#inProp#nodeType#edgeType#prop#value#with#hash#in1#out1");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenNotEnoughParts()
        {
            var result = HasInEdgeProp.IsPredicate("graph1#inProp#nodeType#edgeType#prop#value#in1");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenWrongName()
        {
            var result = HasInEdgeProp.IsPredicate("graph1#outProp#nodeType#edgeType#prop#value#in1#out1");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseForEmptyString()
        {
            var result = HasInEdgeProp.IsPredicate("");
            Assert.IsFalse(result);
        }

        #endregion

        #region Parse Tests

        [TestMethod]
        public void ParseCreatesCorrectObject()
        {
            var result = HasInEdgeProp.Parse("graph1#inProp#nodeType#edgeType#prop#value#in1#out1");
            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("nodeType", result.NodeInTypeName);
            Assert.AreEqual("edgeType", result.EdgeTypeName);
            Assert.AreEqual("prop", result.PropertyName);
            Assert.AreEqual("value", result.PropertyValue);
            Assert.AreEqual("in1", result.NodeInId);
            Assert.AreEqual("out1", result.NodeOutId);
        }

        [TestMethod]
        public void ParseHandlesPropertyValueWithHash()
        {
            var result = HasInEdgeProp.Parse("graph1#inProp#nodeType#edgeType#prop#value#with#hash#in1#out1");
            Assert.AreEqual("value#with#hash", result.PropertyValue);
        }

        [TestMethod]
        public void ParseHandlesEmptyPropertyValue()
        {
            var result = HasInEdgeProp.Parse("graph1#inProp#nodeType#edgeType#prop##in1#out1");
            Assert.AreEqual("", result.PropertyValue);
        }

        [TestMethod]
        public void ParseThrowsWhenNotEnoughParts()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasInEdgeProp.Parse("graph1#inProp#nodeType#edgeType#prop#value#in1"));
        }

        [TestMethod]
        public void ParseThrowsWhenWrongName()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                HasInEdgeProp.Parse("graph1#outProp#nodeType#edgeType#prop#value#in1#out1"));
        }

        #endregion

        #region Static Helper Methods Tests

        [TestMethod]
        public void EdgesByTypeNodeInTypeAndEdgeTypeReturnsCorrectFormat()
        {
            var result = HasInEdgeProp.EdgesByTypeNodeInTypeAndEdgeType("graph1", "nodeType", "edgeType");
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameReturnsCorrectFormat()
        {
            var result = HasInEdgeProp.EdgesByTypeNodeInTypeEdgeTypeAndPropertyName("graph1", "nodeType", "edgeType", "prop");
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#prop#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValueReturnsCorrectFormatForEquals()
        {
            var result = HasInEdgeProp.EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValue(
                "graph1", "nodeType", "edgeType", "prop", PropertyOperator.Equals, "value");
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#prop#value#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValueReturnsCorrectFormatForStartsWith()
        {
            var result = HasInEdgeProp.EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValue(
                "graph1", "nodeType", "edgeType", "prop", PropertyOperator.StartsWith, "value");
            Assert.AreEqual("graph1#inProp#nodeType#edgeType#prop#value", result);
        }

        #endregion

        #region Name Constant Test

        [TestMethod]
        public void NameConstantHasCorrectValue()
        {
            Assert.AreEqual("inProp", HasInEdgeProp.Name);
        }

        #endregion
    }
}
