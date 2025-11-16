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
    public sealed class EdgeByPropCheckTests
    {
        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsEdgeTypeNameProperty()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.AreEqual("edgeType1", check.EdgeTypeName);
        }

        [TestMethod]
        public void ConstructorSetsInIdProperty()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.AreEqual("in1", check.InId);
        }

        [TestMethod]
        public void ConstructorSetsEdgePropNameProperty()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.AreEqual("propName", check.EdgePropName);
        }

        [TestMethod]
        public void ConstructorSetsEdgePropValueProperty()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.AreEqual("propValue", check.EdgePropValue);
        }

        [TestMethod]
        public void ConstructorSetsExistsProperty()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.AreEqual(true, check.Exists);
        }

        [TestMethod]
        public void ConstructorSetsExistsPropertyToFalse()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", false);
            Assert.AreEqual(false, check.Exists);
        }

        #endregion

        #region Equals Tests

        [TestMethod]
        public void EqualsReturnsTrueWhenAllPropertiesMatch()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsTrue(check1.Equals(check2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenEdgeTypeNameDiffers()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType2", "in1", "propName", "propValue", true);
            Assert.IsFalse(check1.Equals(check2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenInIdDiffers()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType1", "in2", "propName", "propValue", true);
            Assert.IsFalse(check1.Equals(check2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenEdgePropNameDiffers()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName1", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType1", "in1", "propName2", "propValue", true);
            Assert.IsFalse(check1.Equals(check2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenEdgePropValueDiffers()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue1", true);
            var check2 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue2", true);
            Assert.IsFalse(check1.Equals(check2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenExistsDiffers()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", false);
            Assert.IsFalse(check1.Equals(check2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithNull()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsFalse(check.Equals((EdgeByPropCheck?)null));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithNullObject()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsFalse(check.Equals((object?)null));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparedWithDifferentType()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsFalse(check.Equals("not an EdgeByPropCheck"));
        }

        [TestMethod]
        public void EqualsReturnsTrueWhenComparedWithSameInstance()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsTrue(check.Equals(check));
        }

        #endregion

        #region GetHashCode Tests

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualObjects()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.AreEqual(check1.GetHashCode(), check2.GetHashCode());
        }

        [TestMethod]
        public void GetHashCodeReturnsDifferentValueForDifferentObjects()
        {
            var check1 = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var check2 = new EdgeByPropCheck("edgeType2", "in2", "propName2", "propValue2", false);
            Assert.AreNotEqual(check1.GetHashCode(), check2.GetHashCode());
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsFormattedString()
        {
            var check = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var result = check.ToString();
            Assert.IsTrue(result.Contains("edgeType1"));
            Assert.IsTrue(result.Contains("in1"));
            Assert.IsTrue(result.Contains("propName"));
            Assert.IsTrue(result.Contains("propValue"));
            Assert.IsTrue(result.Contains("True"));
        }

        [TestMethod]
        public void ToStringHandlesEmptyStrings()
        {
            var check = new EdgeByPropCheck("", "", "", "", false);
            var result = check.ToString();
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Contains("False"));
        }

        #endregion

        #region Operator == Tests

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenBothNull()
        {
            EdgeByPropCheck? left = null;
            EdgeByPropCheck? right = null;
            Assert.IsTrue(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenLeftNull()
        {
            EdgeByPropCheck? left = null;
            EdgeByPropCheck? right = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsFalse(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenRightNull()
        {
            EdgeByPropCheck? left = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            EdgeByPropCheck? right = null;
            Assert.IsFalse(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsTrueWhenEqual()
        {
            var left = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var right = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsTrue(left == right);
        }

        [TestMethod]
        public void EqualityOperatorReturnsFalseWhenNotEqual()
        {
            var left = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var right = new EdgeByPropCheck("edgeType2", "in2", "propName2", "propValue2", false);
            Assert.IsFalse(left == right);
        }

        #endregion

        #region Operator != Tests

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenBothNull()
        {
            EdgeByPropCheck? left = null;
            EdgeByPropCheck? right = null;
            Assert.IsFalse(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenLeftNull()
        {
            EdgeByPropCheck? left = null;
            EdgeByPropCheck? right = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsTrue(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenRightNull()
        {
            EdgeByPropCheck? left = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            EdgeByPropCheck? right = null;
            Assert.IsTrue(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsFalseWhenEqual()
        {
            var left = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var right = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            Assert.IsFalse(left != right);
        }

        [TestMethod]
        public void InequalityOperatorReturnsTrueWhenNotEqual()
        {
            var left = new EdgeByPropCheck("edgeType1", "in1", "propName", "propValue", true);
            var right = new EdgeByPropCheck("edgeType2", "in2", "propName2", "propValue2", false);
            Assert.IsTrue(left != right);
        }

        #endregion
    }
}
