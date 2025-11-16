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
    public sealed class DateTimeFilterExtensionsTests
    {
        #region IsMatch - Eq Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqMatches()
        {
            var filter = new DateTimeFilter { Eq = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 15)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqDoesNotMatch()
        {
            var filter = new DateTimeFilter { Eq = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 16)));
        }

        #endregion

        #region IsMatch - Le Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsEqualToLe()
        {
            var filter = new DateTimeFilter { Le = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 15)));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsLessThanLe()
        {
            var filter = new DateTimeFilter { Le = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 10)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsGreaterThanLe()
        {
            var filter = new DateTimeFilter { Le = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 20)));
        }

        #endregion

        #region IsMatch - Lt Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsLessThanLt()
        {
            var filter = new DateTimeFilter { Lt = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 10)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsEqualToLt()
        {
            var filter = new DateTimeFilter { Lt = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 15)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsGreaterThanLt()
        {
            var filter = new DateTimeFilter { Lt = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 20)));
        }

        #endregion

        #region IsMatch - Ge Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsEqualToGe()
        {
            var filter = new DateTimeFilter { Ge = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 15)));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsGreaterThanGe()
        {
            var filter = new DateTimeFilter { Ge = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 20)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanGe()
        {
            var filter = new DateTimeFilter { Ge = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 10)));
        }

        #endregion

        #region IsMatch - Gt Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsGreaterThanGt()
        {
            var filter = new DateTimeFilter { Gt = new DateTime(2024, 1, 15) };
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 20)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsEqualToGt()
        {
            var filter = new DateTimeFilter { Gt = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 15)));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanGt()
        {
            var filter = new DateTimeFilter { Gt = new DateTime(2024, 1, 15) };
            Assert.IsFalse(filter.IsMatch(new DateTime(2024, 1, 10)));
        }

        #endregion

        #region IsMatch - Empty Filter

        [TestMethod]
        public void IsMatchReturnsTrueWhenNoFiltersAreSet()
        {
            var filter = new DateTimeFilter();
            Assert.IsTrue(filter.IsMatch(new DateTime(2024, 1, 15)));
        }

        #endregion
    }
}
