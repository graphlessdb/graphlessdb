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
    public sealed class EnumFilterExtensionsTests
    {
        #region IsMatch<T> Tests

        [TestMethod]
        public void IsMatchGenericReturnsTrueWhenEqMatches()
        {
            var filter = new EnumFilter<TestStatus> { Eq = TestStatus.Value1 };
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        [TestMethod]
        public void IsMatchGenericReturnsFalseWhenEqDoesNotMatch()
        {
            var filter = new EnumFilter<TestStatus> { Eq = TestStatus.Value1 };
            Assert.IsFalse(filter.IsMatch(TestStatus.Value2));
        }

        [TestMethod]
        public void IsMatchGenericReturnsTrueWhenInContainsValue()
        {
            var filter = new EnumFilter<TestStatus> { In = new[] { TestStatus.Value1, TestStatus.Value2 } };
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        [TestMethod]
        public void IsMatchGenericReturnsFalseWhenInDoesNotContainValue()
        {
            var filter = new EnumFilter<TestStatus> { In = new[] { TestStatus.Value1, TestStatus.Value2 } };
            Assert.IsFalse(filter.IsMatch(TestStatus.Value3));
        }

        [TestMethod]
        public void IsMatchGenericReturnsTrueWhenNoFiltersAreSet()
        {
            var filter = new EnumFilter<TestStatus>();
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        [TestMethod]
        public void IsMatchGenericReturnsFalseWhenEqDoesNotMatchNull()
        {
            var filter = new EnumFilter<TestStatus> { Eq = TestStatus.Value1 };
            Assert.IsFalse(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch (non-generic) Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqMatches()
        {
            var filter = new EnumFilter { Eq = TestStatus.Value1 };
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqDoesNotMatch()
        {
            var filter = new EnumFilter { Eq = TestStatus.Value1 };
            Assert.IsFalse(filter.IsMatch(TestStatus.Value2));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqDoesNotMatchNull()
        {
            var filter = new EnumFilter { Eq = TestStatus.Value1 };
            Assert.IsFalse(filter.IsMatch(null));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenInContainsValue()
        {
            var filter = new EnumFilter { In = new object[] { TestStatus.Value1, TestStatus.Value2 } };
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenInDoesNotContainValue()
        {
            var filter = new EnumFilter { In = new object[] { TestStatus.Value1, TestStatus.Value2 } };
            Assert.IsFalse(filter.IsMatch(TestStatus.Value3));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenInDoesNotContainNull()
        {
            var filter = new EnumFilter { In = new object[] { TestStatus.Value1, TestStatus.Value2 } };
            Assert.IsFalse(filter.IsMatch(null));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenNoFiltersAreSet()
        {
            var filter = new EnumFilter();
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqIsNullAndValueIsNull()
        {
            var filter = new EnumFilter { Eq = null };
            Assert.IsTrue(filter.IsMatch(null));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenInIsNullAndValueIsAny()
        {
            var filter = new EnumFilter { In = null };
            Assert.IsTrue(filter.IsMatch(TestStatus.Value1));
        }

        #endregion
    }
}
