/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class IdFilterExtensionsTests
    {
        #region IsMatch - Eq Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqMatches()
        {
            var filter = new IdFilter { Eq = "test-id" };
            Assert.IsTrue(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqDoesNotMatch()
        {
            var filter = new IdFilter { Eq = "test-id" };
            Assert.IsFalse(filter.IsMatch("other-id"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqIsNull()
        {
            var filter = new IdFilter { Eq = null };
            Assert.IsTrue(filter.IsMatch("any-id"));
        }

        #endregion

        #region IsMatch - Ne Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenNeDoesNotMatch()
        {
            var filter = new IdFilter { Ne = "test-id" };
            Assert.IsTrue(filter.IsMatch("other-id"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenNeMatches()
        {
            var filter = new IdFilter { Ne = "test-id" };
            Assert.IsFalse(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenNeIsNull()
        {
            var filter = new IdFilter { Ne = null };
            Assert.IsTrue(filter.IsMatch("any-id"));
        }

        #endregion

        #region IsMatch - In Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsInArray()
        {
            var filter = new IdFilter { In = new[] { "id-1", "id-2", "id-3" } };
            Assert.IsTrue(filter.IsMatch("id-2"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsNotInArray()
        {
            var filter = new IdFilter { In = new[] { "id-1", "id-2", "id-3" } };
            Assert.IsFalse(filter.IsMatch("id-4"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenInIsNull()
        {
            var filter = new IdFilter { In = null };
            Assert.IsTrue(filter.IsMatch("any-id"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenInIsEmptyArray()
        {
            var filter = new IdFilter { In = System.Array.Empty<string>() };
            Assert.IsFalse(filter.IsMatch("any-id"));
        }

        #endregion

        #region IsMatch - Empty Filter

        [TestMethod]
        public void IsMatchReturnsTrueWhenNoFiltersAreSet()
        {
            var filter = new IdFilter();
            Assert.IsTrue(filter.IsMatch("any-id"));
        }

        #endregion

        #region IsMatch - Combined Filters

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqAndNeAreCompatible()
        {
            var filter = new IdFilter { Eq = "test-id", Ne = "other-id" };
            Assert.IsTrue(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqAndNeConflict()
        {
            var filter = new IdFilter { Eq = "test-id", Ne = "test-id" };
            Assert.IsFalse(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqAndInAreCompatible()
        {
            var filter = new IdFilter { Eq = "test-id", In = new[] { "test-id", "other-id" } };
            Assert.IsTrue(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqMatchesButInDoesNot()
        {
            var filter = new IdFilter { Eq = "test-id", In = new[] { "id-1", "id-2" } };
            Assert.IsFalse(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenInAndNeAreCompatible()
        {
            var filter = new IdFilter { In = new[] { "id-1", "id-2", "id-3" }, Ne = "id-4" };
            Assert.IsTrue(filter.IsMatch("id-2"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenInMatchesButNeDoesNot()
        {
            var filter = new IdFilter { In = new[] { "id-1", "id-2", "id-3" }, Ne = "id-2" };
            Assert.IsFalse(filter.IsMatch("id-2"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenAllFiltersAreCompatible()
        {
            var filter = new IdFilter { Eq = "test-id", Ne = "other-id", In = new[] { "test-id", "another-id" } };
            Assert.IsTrue(filter.IsMatch("test-id"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenAnyFilterFails()
        {
            var filter = new IdFilter { Eq = "test-id", Ne = "other-id", In = new[] { "id-1", "id-2" } };
            Assert.IsFalse(filter.IsMatch("test-id"));
        }

        #endregion
    }
}
