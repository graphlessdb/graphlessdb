/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class FuncEqualityComparerTests
    {
        [TestMethod]
        public void ConstructorWithBothParametersStoresComparerAndHash()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;
            Func<string, int> hash = x => x?.GetHashCode() ?? 0;

            var equalityComparer = new FuncEqualityComparer<string>(comparer, hash);

            Assert.IsNotNull(equalityComparer);
        }

        [TestMethod]
        public void ConstructorWithOnlyComparerUsesDefaultHash()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;

            var equalityComparer = new FuncEqualityComparer<string>(comparer);

            Assert.IsNotNull(equalityComparer);
        }

        [TestMethod]
        public void EqualsReturnsTrueWhenComparerReturnsTrue()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;
            var equalityComparer = new FuncEqualityComparer<string>(comparer);

            var result = equalityComparer.Equals("test", "test");

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenComparerReturnsFalse()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;
            var equalityComparer = new FuncEqualityComparer<string>(comparer);

            var result = equalityComparer.Equals("test", "different");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void GetHashCodeReturnsHashFromProvidedFunction()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;
            Func<string, int> hash = x => x.Length * 2;
            var equalityComparer = new FuncEqualityComparer<string>(comparer, hash);

            var result = equalityComparer.GetHashCode("test");

            Assert.AreEqual(8, result);
        }

        [TestMethod]
        public void GetHashCodeReturnsZeroWhenUsingConstructorWithoutHash()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;
            var equalityComparer = new FuncEqualityComparer<string>(comparer);

            var result = equalityComparer.GetHashCode("test");

            Assert.AreEqual(0, result);
        }

        [TestMethod]
        public void EqualsWorksWithNullValues()
        {
            Func<string?, string?, bool> comparer = (x, y) => x == y;
            var equalityComparer = new FuncEqualityComparer<string?>(comparer);

            var resultBothNull = equalityComparer.Equals(null, null);
            var resultOneNull = equalityComparer.Equals(null, "test");
            var resultOtherNull = equalityComparer.Equals("test", null);

            Assert.IsTrue(resultBothNull);
            Assert.IsFalse(resultOneNull);
            Assert.IsFalse(resultOtherNull);
        }

        [TestMethod]
        public void EqualsUsesCustomComparerLogic()
        {
            Func<string?, string?, bool> comparer = (x, y) =>
                string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
            var equalityComparer = new FuncEqualityComparer<string?>(comparer);

            var result = equalityComparer.Equals("TEST", "test");

            Assert.IsTrue(result);
        }
    }
}
