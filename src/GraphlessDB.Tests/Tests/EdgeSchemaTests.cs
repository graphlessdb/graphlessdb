/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class EdgeSchemaTests
    {
        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsNameProperty()
        {
            var schema = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreEqual("EdgeName", schema.Name);
        }

        [TestMethod]
        public void ConstructorSetsNodeInTypeProperty()
        {
            var schema = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreEqual("NodeIn", schema.NodeInType);
        }

        [TestMethod]
        public void ConstructorSetsNodeInCardinalityProperty()
        {
            var schema = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreEqual(EdgeCardinality.One, schema.NodeInCardinality);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutTypeProperty()
        {
            var schema = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreEqual("NodeOut", schema.NodeOutType);
        }

        [TestMethod]
        public void ConstructorSetsNodeOutCardinalityProperty()
        {
            var schema = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreEqual(EdgeCardinality.ZeroOrMany, schema.NodeOutCardinality);
        }

        #endregion

        #region Equals Tests

        [TestMethod]
        public void EqualsReturnsTrueWhenAllPropertiesMatch()
        {
            var schema1 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsTrue(schema1.Equals(schema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNameDiffers()
        {
            var schema1 = new EdgeSchema("EdgeName1", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName2", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(schema1.Equals(schema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNodeInTypeDiffers()
        {
            var schema1 = new EdgeSchema("EdgeName", "NodeIn1", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName", "NodeIn2", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(schema1.Equals(schema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNodeInCardinalityDiffers()
        {
            var schema1 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.ZeroOrOne, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(schema1.Equals(schema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNodeOutTypeDiffers()
        {
            var schema1 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut1", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut2", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(schema1.Equals(schema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNodeOutCardinalityDiffers()
        {
            var schema1 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.OneOrMany);
            Assert.IsFalse(schema1.Equals(schema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithNull()
        {
            var schema = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(schema.Equals(null));
        }

        #endregion

        #region GetHashCode Tests

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualObjects()
        {
            var schema1 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreEqual(schema1.GetHashCode(), schema2.GetHashCode());
        }

        [TestMethod]
        public void GetHashCodeReturnsDifferentValueForDifferentObjects()
        {
            var schema1 = new EdgeSchema("EdgeName1", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var schema2 = new EdgeSchema("EdgeName2", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.AreNotEqual(schema1.GetHashCode(), schema2.GetHashCode());
        }

        #endregion

        #region Operator == Tests

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenBothNull()
        {
            EdgeSchema? left = null;
            EdgeSchema? right = null;
            Assert.IsTrue(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenLeftNull()
        {
            EdgeSchema? left = null;
            EdgeSchema? right = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenRightNull()
        {
            EdgeSchema? left = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            EdgeSchema? right = null;
            Assert.IsFalse(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenEqual()
        {
            var left = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var right = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsTrue(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenNotEqual()
        {
            var left = new EdgeSchema("EdgeName1", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var right = new EdgeSchema("EdgeName2", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(left == right);
        }

        #endregion

        #region Operator != Tests

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenBothNull()
        {
            EdgeSchema? left = null;
            EdgeSchema? right = null;
            Assert.IsFalse(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenLeftNull()
        {
            EdgeSchema? left = null;
            EdgeSchema? right = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsTrue(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenRightNull()
        {
            EdgeSchema? left = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            EdgeSchema? right = null;
            Assert.IsTrue(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenEqual()
        {
            var left = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var right = new EdgeSchema("EdgeName", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsFalse(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenNotEqual()
        {
            var left = new EdgeSchema("EdgeName1", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            var right = new EdgeSchema("EdgeName2", "NodeIn", EdgeCardinality.One, "NodeOut", EdgeCardinality.ZeroOrMany);
            Assert.IsTrue(left != right);
        }

        #endregion
    }
}
