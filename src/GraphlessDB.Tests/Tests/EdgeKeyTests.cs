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
    public sealed class EdgeKeyTests
    {
        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsTypeNameProperty()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual("type1", edgeKey.TypeName);
        }

        [TestMethod]
        public void ConstructorSetsInIdProperty()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual("in1", edgeKey.InId);
        }

        [TestMethod]
        public void ConstructorSetsOutIdProperty()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual("out1", edgeKey.OutId);
        }

        #endregion

        #region CompareTo Tests

        [TestMethod]
        public void CompareToReturnsNegativeOneWhenOtherIsNull()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual(-1, edgeKey.CompareTo(null));
        }

        [TestMethod]
        public void CompareToReturnsZeroWhenEqual()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual(0, edgeKey1.CompareTo(edgeKey2));
        }

        [TestMethod]
        public void CompareToReturnsNegativeWhenTypeNameIsLess()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type2", "in1", "out1");
            Assert.IsTrue(edgeKey1.CompareTo(edgeKey2) < 0);
        }

        [TestMethod]
        public void CompareToReturnsPositiveWhenTypeNameIsGreater()
        {
            var edgeKey1 = new EdgeKey("type2", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(edgeKey1.CompareTo(edgeKey2) > 0);
        }

        [TestMethod]
        public void CompareToReturnsNegativeWhenInIdIsLess()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in2", "out1");
            Assert.IsTrue(edgeKey1.CompareTo(edgeKey2) < 0);
        }

        [TestMethod]
        public void CompareToReturnsPositiveWhenInIdIsGreater()
        {
            var edgeKey1 = new EdgeKey("type1", "in2", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(edgeKey1.CompareTo(edgeKey2) > 0);
        }

        [TestMethod]
        public void CompareToReturnsNegativeWhenOutIdIsLess()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out2");
            Assert.IsTrue(edgeKey1.CompareTo(edgeKey2) < 0);
        }

        [TestMethod]
        public void CompareToReturnsPositiveWhenOutIdIsGreater()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out2");
            var edgeKey2 = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(edgeKey1.CompareTo(edgeKey2) > 0);
        }

        #endregion

        #region Equals Tests

        [TestMethod]
        public void EqualsReturnsTrueWhenAllPropertiesMatch()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(edgeKey1.Equals(edgeKey2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenTypeNameDiffers()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type2", "in1", "out1");
            Assert.IsFalse(edgeKey1.Equals(edgeKey2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenInIdDiffers()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in2", "out1");
            Assert.IsFalse(edgeKey1.Equals(edgeKey2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenOutIdDiffers()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out2");
            Assert.IsFalse(edgeKey1.Equals(edgeKey2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithNull()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(edgeKey.Equals((EdgeKey?)null));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithNullObject()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(edgeKey.Equals((object?)null));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithDifferentType()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(edgeKey.Equals("not an EdgeKey"));
        }

        [TestMethod]
        public void EqualsReturnsTrueWhenComparedWithSameInstance()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(edgeKey.Equals(edgeKey));
        }

        #endregion

        #region GetHashCode Tests

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualObjects()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual(edgeKey1.GetHashCode(), edgeKey2.GetHashCode());
        }

        [TestMethod]
        public void GetHashCodeReturnsDifferentValueForDifferentObjects()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type2", "in2", "out2");
            Assert.AreNotEqual(edgeKey1.GetHashCode(), edgeKey2.GetHashCode());
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsFormattedString()
        {
            var edgeKey = new EdgeKey("type1", "in1", "out1");
            Assert.AreEqual("type1#in1#out1", edgeKey.ToString());
        }

        [TestMethod]
        public void ToStringHandlesEmptyStrings()
        {
            var edgeKey = new EdgeKey("", "", "");
            Assert.AreEqual("##", edgeKey.ToString());
        }

        #endregion

        #region Operator == Tests

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenBothNull()
        {
            EdgeKey? left = null;
            EdgeKey? right = null;
            Assert.IsTrue(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenLeftNull()
        {
            EdgeKey? left = null;
            EdgeKey? right = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenRightNull()
        {
            EdgeKey? left = new EdgeKey("type1", "in1", "out1");
            EdgeKey? right = null;
            Assert.IsFalse(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenNotEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type2", "in2", "out2");
            Assert.IsFalse(left == right);
        }

        #endregion

        #region Operator != Tests

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenBothNull()
        {
            EdgeKey? left = null;
            EdgeKey? right = null;
            Assert.IsFalse(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenLeftNull()
        {
            EdgeKey? left = null;
            EdgeKey? right = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenRightNull()
        {
            EdgeKey? left = new EdgeKey("type1", "in1", "out1");
            EdgeKey? right = null;
            Assert.IsTrue(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenNotEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type2", "in2", "out2");
            Assert.IsTrue(left != right);
        }

        #endregion

        #region Operator < Tests

        [TestMethod]
        public void LessThanOperatorReturnsTrueWhenLeftIsLess()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type2", "in1", "out1");
            Assert.IsTrue(left < right);
        }

        [TestMethod]
        public void LessThanOperatorReturnsFalseWhenLeftIsGreater()
        {
            var left = new EdgeKey("type2", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(left < right);
        }

        [TestMethod]
        public void LessThanOperatorReturnsFalseWhenEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(left < right);
        }

        #endregion

        #region Operator > Tests

        [TestMethod]
        public void GreaterThanOperatorReturnsTrueWhenLeftIsGreater()
        {
            var left = new EdgeKey("type2", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(left > right);
        }

        [TestMethod]
        public void GreaterThanOperatorReturnsFalseWhenLeftIsLess()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type2", "in1", "out1");
            Assert.IsFalse(left > right);
        }

        [TestMethod]
        public void GreaterThanOperatorReturnsFalseWhenEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(left > right);
        }

        #endregion

        #region Operator <= Tests

        [TestMethod]
        public void LessThanOrEqualOperatorReturnsTrueWhenLeftIsLess()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type2", "in1", "out1");
            Assert.IsTrue(left <= right);
        }

        [TestMethod]
        public void LessThanOrEqualOperatorReturnsTrueWhenEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(left <= right);
        }

        [TestMethod]
        public void LessThanOrEqualOperatorReturnsFalseWhenLeftIsGreater()
        {
            var left = new EdgeKey("type2", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsFalse(left <= right);
        }

        #endregion

        #region Operator >= Tests

        [TestMethod]
        public void GreaterThanOrEqualOperatorReturnsTrueWhenLeftIsGreater()
        {
            var left = new EdgeKey("type2", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(left >= right);
        }

        [TestMethod]
        public void GreaterThanOrEqualOperatorReturnsTrueWhenEqual()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type1", "in1", "out1");
            Assert.IsTrue(left >= right);
        }

        [TestMethod]
        public void GreaterThanOrEqualOperatorReturnsFalseWhenLeftIsLess()
        {
            var left = new EdgeKey("type1", "in1", "out1");
            var right = new EdgeKey("type2", "in1", "out1");
            Assert.IsFalse(left >= right);
        }

        #endregion
    }
}
