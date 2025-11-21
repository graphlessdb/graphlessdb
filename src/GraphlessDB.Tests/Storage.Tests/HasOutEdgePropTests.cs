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
    public sealed class HasOutEdgePropTests
    {
        [TestMethod]
        public void CanCreateWithValidParameters()
        {
            var hasOutEdgeProp = new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");

            Assert.AreEqual("graph1", hasOutEdgeProp.GraphName);
            Assert.AreEqual("nodeOutType1", hasOutEdgeProp.NodeOutTypeName);
            Assert.AreEqual("edgeType1", hasOutEdgeProp.EdgeTypeName);
            Assert.AreEqual("prop1", hasOutEdgeProp.PropertyName);
            Assert.AreEqual("value1", hasOutEdgeProp.PropertyValue);
            Assert.AreEqual("nodeIn1", hasOutEdgeProp.NodeInId);
            Assert.AreEqual("nodeOut1", hasOutEdgeProp.NodeOutId);
        }

        [TestMethod]
        public void CanCreateWithNullPropertyValue()
        {
#pragma warning disable CS8625
            var hasOutEdgeProp = new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", null, "nodeIn1", "nodeOut1");
#pragma warning restore CS8625

            Assert.IsNull(hasOutEdgeProp.PropertyValue);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullGraphName()
        {
#pragma warning disable CS8625
            new HasOutEdgeProp(null, "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyGraphName()
        {
            new HasOutEdgeProp(string.Empty, "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceGraphName()
        {
            new HasOutEdgeProp("   ", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithGraphNameContainingHash()
        {
            new HasOutEdgeProp("graph#1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullNodeOutTypeName()
        {
#pragma warning disable CS8625
            new HasOutEdgeProp("graph1", null, "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyNodeOutTypeName()
        {
            new HasOutEdgeProp("graph1", string.Empty, "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceNodeOutTypeName()
        {
            new HasOutEdgeProp("graph1", "   ", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNodeOutTypeNameContainingHash()
        {
            new HasOutEdgeProp("graph1", "nodeOutType#1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullEdgeTypeName()
        {
#pragma warning disable CS8625
            new HasOutEdgeProp("graph1", "nodeOutType1", null, "prop1", "value1", "nodeIn1", "nodeOut1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyEdgeTypeName()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", string.Empty, "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceEdgeTypeName()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "   ", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEdgeTypeNameContainingHash()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType#1", "prop1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullPropertyName()
        {
#pragma warning disable CS8625
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", null, "value1", "nodeIn1", "nodeOut1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyPropertyName()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", string.Empty, "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespacePropertyName()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "   ", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithPropertyNameContainingHash()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop#1", "value1", "nodeIn1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullNodeInId()
        {
#pragma warning disable CS8625
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", null, "nodeOut1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyNodeInId()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", string.Empty, "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceNodeInId()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "   ", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNodeInIdContainingHash()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn#1", "nodeOut1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullNodeOutId()
        {
#pragma warning disable CS8625
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", null);
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyNodeOutId()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", string.Empty);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceNodeOutId()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "   ");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNodeOutIdContainingHash()
        {
            new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut#1");
        }

        [TestMethod]
        public void CanConvertToString()
        {
            var hasOutEdgeProp = new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value1", "nodeIn1", "nodeOut1");

            var result = hasOutEdgeProp.ToString();

            Assert.AreEqual("graph1#outProp#nodeOutType1#edgeType1#prop1#value1#nodeIn1#nodeOut1", result);
        }

        [TestMethod]
        public void CanConvertToStringWithPropertyValueContainingHash()
        {
            var hasOutEdgeProp = new HasOutEdgeProp("graph1", "nodeOutType1", "edgeType1", "prop1", "value#with#hashes", "nodeIn1", "nodeOut1");

            var result = hasOutEdgeProp.ToString();

            Assert.AreEqual("graph1#outProp#nodeOutType1#edgeType1#prop1#value#with#hashes#nodeIn1#nodeOut1", result);
        }

        [TestMethod]
        public void IsPredicateReturnsTrueForValidValue()
        {
            var result = HasOutEdgeProp.IsPredicate("graph1#outProp#nodeOutType1#edgeType1#prop1#value1#nodeIn1#nodeOut1");

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseForInsufficientParts()
        {
            var result = HasOutEdgeProp.IsPredicate("graph1#outProp#nodeOutType1");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseForIncorrectPredicateName()
        {
            var result = HasOutEdgeProp.IsPredicate("graph1#wrongProp#nodeOutType1#edgeType1#prop1#value1#nodeIn1#nodeOut1");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsTrueForValueWithHashesInPropertyValue()
        {
            var result = HasOutEdgeProp.IsPredicate("graph1#outProp#nodeOutType1#edgeType1#prop1#value#with#hashes#nodeIn1#nodeOut1");

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanParseValidString()
        {
            var result = HasOutEdgeProp.Parse("graph1#outProp#nodeOutType1#edgeType1#prop1#value1#nodeIn1#nodeOut1");

            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("nodeOutType1", result.NodeOutTypeName);
            Assert.AreEqual("edgeType1", result.EdgeTypeName);
            Assert.AreEqual("prop1", result.PropertyName);
            Assert.AreEqual("value1", result.PropertyValue);
            Assert.AreEqual("nodeIn1", result.NodeInId);
            Assert.AreEqual("nodeOut1", result.NodeOutId);
        }

        [TestMethod]
        public void CanParseStringWithHashesInPropertyValue()
        {
            var result = HasOutEdgeProp.Parse("graph1#outProp#nodeOutType1#edgeType1#prop1#value#with#hashes#nodeIn1#nodeOut1");

            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("nodeOutType1", result.NodeOutTypeName);
            Assert.AreEqual("edgeType1", result.EdgeTypeName);
            Assert.AreEqual("prop1", result.PropertyName);
            Assert.AreEqual("value#with#hashes", result.PropertyValue);
            Assert.AreEqual("nodeIn1", result.NodeInId);
            Assert.AreEqual("nodeOut1", result.NodeOutId);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotParseStringWithInsufficientParts()
        {
            HasOutEdgeProp.Parse("graph1#outProp#nodeOutType1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotParseStringWithIncorrectPredicateName()
        {
            HasOutEdgeProp.Parse("graph1#wrongProp#nodeOutType1#edgeType1#prop1#value1#nodeIn1#nodeOut1");
        }

        [TestMethod]
        public void EdgesByTypeNodeOutTypeAndEdgeTypeReturnsCorrectFormat()
        {
            var result = HasOutEdgeProp.EdgesByTypeNodeOutTypeAndEdgeType("graph1", "nodeOutType1", "edgeType1");

            Assert.AreEqual("graph1#outProp#nodeOutType1#edgeType1#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameReturnsCorrectFormat()
        {
            var result = HasOutEdgeProp.EdgesByTypeNodeOutTypeEdgeTypeAndPropertyName("graph1", "nodeOutType1", "edgeType1", "prop1");

            Assert.AreEqual("graph1#outProp#nodeOutType1#edgeType1#prop1#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValueReturnsCorrectFormatForEquals()
        {
            var result = HasOutEdgeProp.EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValue(
                "graph1", "nodeOutType1", "edgeType1", "prop1", PropertyOperator.Equals, "value1");

            Assert.AreEqual("graph1#outProp#nodeOutType1#edgeType1#prop1#value1#", result);
        }

        [TestMethod]
        public void EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValueReturnsCorrectFormatForStartsWith()
        {
            var result = HasOutEdgeProp.EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValue(
                "graph1", "nodeOutType1", "edgeType1", "prop1", PropertyOperator.StartsWith, "value1");

            Assert.AreEqual("graph1#outProp#nodeOutType1#edgeType1#prop1#value1", result);
        }
    }
}
