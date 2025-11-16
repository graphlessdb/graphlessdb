/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using GraphlessDB.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class EnumerableExtensionsTests
    {
        #region WhereNotNull - Class Tests

        [TestMethod]
        public void WhereNotNullClassFiltersOutNullValues()
        {
            var source = new List<string?> { "a", null, "b", null, "c" };
            var result = source.WhereNotNull().ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<string> { "a", "b", "c" }, result);
        }

        [TestMethod]
        public void WhereNotNullClassReturnsEmptyWhenAllNull()
        {
            var source = new List<string?> { null, null, null };
            var result = source.WhereNotNull().ToList();

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void WhereNotNullClassReturnsAllWhenNoneNull()
        {
            var source = new List<string> { "a", "b", "c" };
            var result = source.WhereNotNull().ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<string> { "a", "b", "c" }, result);
        }

        #endregion

        #region WhereNotNull - Struct Tests

        [TestMethod]
        public void WhereNotNullStructFiltersOutNullValues()
        {
            var source = new List<int?> { 1, null, 2, null, 3 };
            var result = source.WhereNotNull().ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result);
        }

        [TestMethod]
        public void WhereNotNullStructReturnsEmptyWhenAllNull()
        {
            var source = new List<int?> { null, null, null };
            var result = source.WhereNotNull().ToList();

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void WhereNotNullStructReturnsAllWhenNoneNull()
        {
            var source = new List<int?> { 1, 2, 3 };
            var result = source.WhereNotNull().ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result);
        }

        #endregion

        #region NotNull - Class Tests

        [TestMethod]
        public void NotNullClassReturnsAllValuesWhenNoneNull()
        {
            var source = new List<string> { "a", "b", "c" };
            var result = source.NotNull().ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<string> { "a", "b", "c" }, result);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void NotNullClassThrowsWhenNullEncountered()
        {
            var source = new List<string?> { "a", null, "b" };
            var result = source.NotNull().ToList();
        }

        #endregion

        #region NotNull - Struct Tests

        [TestMethod]
        public void NotNullStructReturnsAllValuesWhenNoneNull()
        {
            var source = new List<int?> { 1, 2, 3 };
            var result = source.NotNull().ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void NotNullStructThrowsWhenNullEncountered()
        {
            var source = new List<int?> { 1, null, 2 };
            var result = source.NotNull().ToList();
        }

        #endregion

        #region ToDictionary Tests

        [TestMethod]
        public void ToDictionaryConvertsImmutableDictionaryToRegularDictionary()
        {
            var source = ImmutableDictionary<string, int>.Empty
                .Add("a", 1)
                .Add("b", 2)
                .Add("c", 3);

            var result = source.ToDictionary();

            Assert.AreEqual(3, result.Count);
            Assert.AreEqual(1, result["a"]);
            Assert.AreEqual(2, result["b"]);
            Assert.AreEqual(3, result["c"]);
        }

        [TestMethod]
        public void ToDictionaryReturnsEmptyDictionaryForEmptySource()
        {
            var source = ImmutableDictionary<string, int>.Empty;
            var result = source.ToDictionary();

            Assert.AreEqual(0, result.Count);
        }

        #endregion

        #region OrderByDirection Tests

        [TestMethod]
        public void OrderByDirectionAscendingOrdersCorrectly()
        {
            var source = new List<int> { 3, 1, 4, 1, 5, 9, 2, 6 };
            var result = source.OrderByDirection(x => x, descending: false).ToList();

            CollectionAssert.AreEqual(new List<int> { 1, 1, 2, 3, 4, 5, 6, 9 }, result);
        }

        [TestMethod]
        public void OrderByDirectionDescendingOrdersCorrectly()
        {
            var source = new List<int> { 3, 1, 4, 1, 5, 9, 2, 6 };
            var result = source.OrderByDirection(x => x, descending: true).ToList();

            CollectionAssert.AreEqual(new List<int> { 9, 6, 5, 4, 3, 2, 1, 1 }, result);
        }

        [TestMethod]
        public void OrderByDirectionWithComparerWorks()
        {
            var source = new List<string> { "apple", "Banana", "cherry", "Date" };
            var result = source.OrderByDirection(x => x, descending: false, StringComparer.OrdinalIgnoreCase).ToList();

            CollectionAssert.AreEqual(new List<string> { "apple", "Banana", "cherry", "Date" }, result);
        }

        #endregion

        #region ToListBatches Tests

        [TestMethod]
        public void ToListBatchesCreatesCorrectBatches()
        {
            var source = Enumerable.Range(1, 10);
            var result = source.ToListBatches(3).ToList();

            Assert.AreEqual(4, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result[0]);
            CollectionAssert.AreEqual(new List<int> { 4, 5, 6 }, result[1]);
            CollectionAssert.AreEqual(new List<int> { 7, 8, 9 }, result[2]);
            CollectionAssert.AreEqual(new List<int> { 10 }, result[3]);
        }

        [TestMethod]
        public void ToListBatchesHandlesExactMultiple()
        {
            var source = Enumerable.Range(1, 9);
            var result = source.ToListBatches(3).ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result[0]);
            CollectionAssert.AreEqual(new List<int> { 4, 5, 6 }, result[1]);
            CollectionAssert.AreEqual(new List<int> { 7, 8, 9 }, result[2]);
        }

        [TestMethod]
        public void ToListBatchesHandlesEmptySource()
        {
            var source = Enumerable.Empty<int>();
            var result = source.ToListBatches(3).ToList();

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void ToListBatchesHandlesSingleBatch()
        {
            var source = new List<int> { 1, 2 };
            var result = source.ToListBatches(5).ToList();

            Assert.AreEqual(1, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2 }, result[0]);
        }

        #endregion

        #region ToImmutableListBatches Tests

        [TestMethod]
        public void ToImmutableListBatchesCreatesCorrectBatches()
        {
            var source = Enumerable.Range(1, 10);
            var result = source.ToImmutableListBatches(3).ToList();

            Assert.AreEqual(4, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result[0].ToList());
            CollectionAssert.AreEqual(new List<int> { 4, 5, 6 }, result[1].ToList());
            CollectionAssert.AreEqual(new List<int> { 7, 8, 9 }, result[2].ToList());
            CollectionAssert.AreEqual(new List<int> { 10 }, result[3].ToList());
        }

        [TestMethod]
        public void ToImmutableListBatchesHandlesExactMultiple()
        {
            var source = Enumerable.Range(1, 9);
            var result = source.ToImmutableListBatches(3).ToList();

            Assert.AreEqual(3, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result[0].ToList());
            CollectionAssert.AreEqual(new List<int> { 4, 5, 6 }, result[1].ToList());
            CollectionAssert.AreEqual(new List<int> { 7, 8, 9 }, result[2].ToList());
        }

        [TestMethod]
        public void ToImmutableListBatchesHandlesEmptySource()
        {
            var source = Enumerable.Empty<int>();
            var result = source.ToImmutableListBatches(3).ToList();

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void ToImmutableListBatchesHandlesSingleBatch()
        {
            var source = new List<int> { 1, 2 };
            var result = source.ToImmutableListBatches(5).ToList();

            Assert.AreEqual(1, result.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2 }, result[0].ToList());
        }

        #endregion

        #region CompareTo HashSet Tests

        [TestMethod]
        public void CompareToHashSetReturnsCorrectMatches()
        {
            var set1 = ImmutableHashSet<int>.Empty.Add(1).Add(2).Add(3);
            var set2 = ImmutableHashSet<int>.Empty.Add(2).Add(3).Add(4);

            var result = set1.CompareTo(set2);

            Assert.AreEqual(2, result.Match.Count);
            Assert.IsTrue(result.Match.Contains(2));
            Assert.IsTrue(result.Match.Contains(3));
        }

        [TestMethod]
        public void CompareToHashSetReturnsCorrectOnlyIn1()
        {
            var set1 = ImmutableHashSet<int>.Empty.Add(1).Add(2).Add(3);
            var set2 = ImmutableHashSet<int>.Empty.Add(2).Add(3).Add(4);

            var result = set1.CompareTo(set2);

            Assert.AreEqual(1, result.OnlyIn1.Count);
            Assert.IsTrue(result.OnlyIn1.Contains(1));
        }

        [TestMethod]
        public void CompareToHashSetReturnsCorrectOnlyIn2()
        {
            var set1 = ImmutableHashSet<int>.Empty.Add(1).Add(2).Add(3);
            var set2 = ImmutableHashSet<int>.Empty.Add(2).Add(3).Add(4);

            var result = set1.CompareTo(set2);

            Assert.AreEqual(1, result.OnlyIn2.Count);
            Assert.IsTrue(result.OnlyIn2.Contains(4));
        }

        [TestMethod]
        public void CompareToHashSetHandlesEmptySets()
        {
            var set1 = ImmutableHashSet<int>.Empty;
            var set2 = ImmutableHashSet<int>.Empty;

            var result = set1.CompareTo(set2);

            Assert.AreEqual(0, result.Match.Count);
            Assert.AreEqual(0, result.OnlyIn1.Count);
            Assert.AreEqual(0, result.OnlyIn2.Count);
        }

        [TestMethod]
        public void CompareToHashSetHandlesCompletelyDifferentSets()
        {
            var set1 = ImmutableHashSet<int>.Empty.Add(1).Add(2);
            var set2 = ImmutableHashSet<int>.Empty.Add(3).Add(4);

            var result = set1.CompareTo(set2);

            Assert.AreEqual(0, result.Match.Count);
            Assert.AreEqual(2, result.OnlyIn1.Count);
            Assert.AreEqual(2, result.OnlyIn2.Count);
        }

        #endregion

        #region CompareTo Dictionary Tests

        [TestMethod]
        public void CompareToDictionaryReturnsCorrectMatches()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty.Add("a", 1).Add("b", 2).Add("c", 3);
            var dict2 = ImmutableDictionary<string, int>.Empty.Add("b", 2).Add("c", 3).Add("d", 4);

            var result = dict1.CompareTo(dict2, v1 => v1, v2 => v2);

            Assert.AreEqual(2, result.Match.Count);
            Assert.AreEqual(2, result.Match["b"]);
            Assert.AreEqual(3, result.Match["c"]);
        }

        [TestMethod]
        public void CompareToDictionaryReturnsCorrectDifferent()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty.Add("a", 1).Add("b", 2);
            var dict2 = ImmutableDictionary<string, int>.Empty.Add("a", 1).Add("b", 5);

            var result = dict1.CompareTo(dict2, v1 => v1, v2 => v2);

            Assert.AreEqual(1, result.Different.Count);
            Assert.IsTrue(result.Different.ContainsKey("b"));
            Assert.AreEqual(2, result.Different["b"].Item1);
            Assert.AreEqual(5, result.Different["b"].Item2);
        }

        [TestMethod]
        public void CompareToDictionaryReturnsCorrectOnlyIn1()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty.Add("a", 1).Add("b", 2);
            var dict2 = ImmutableDictionary<string, int>.Empty.Add("b", 2).Add("c", 3);

            var result = dict1.CompareTo(dict2, v1 => v1, v2 => v2);

            Assert.AreEqual(1, result.OnlyIn1.Count);
            Assert.AreEqual(1, result.OnlyIn1["a"]);
        }

        [TestMethod]
        public void CompareToDictionaryReturnsCorrectOnlyIn2()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty.Add("a", 1).Add("b", 2);
            var dict2 = ImmutableDictionary<string, int>.Empty.Add("b", 2).Add("c", 3);

            var result = dict1.CompareTo(dict2, v1 => v1, v2 => v2);

            Assert.AreEqual(1, result.OnlyIn2.Count);
            Assert.AreEqual(3, result.OnlyIn2["c"]);
        }

        [TestMethod]
        public void CompareToDictionaryHandlesEmptyDictionaries()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty;
            var dict2 = ImmutableDictionary<string, int>.Empty;

            var result = dict1.CompareTo(dict2, v1 => v1, v2 => v2);

            Assert.AreEqual(0, result.Match.Count);
            Assert.AreEqual(0, result.Different.Count);
            Assert.AreEqual(0, result.OnlyIn1.Count);
            Assert.AreEqual(0, result.OnlyIn2.Count);
        }

        [TestMethod]
        public void CompareToDictionaryWithDifferentValueTypes()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty.Add("a", 1).Add("b", 2);
            var dict2 = ImmutableDictionary<string, string>.Empty.Add("a", "1").Add("b", "2");

            var result = dict1.CompareTo(dict2, v1 => v1.ToString(CultureInfo.InvariantCulture), v2 => v2);

            Assert.AreEqual(2, result.Match.Count);
            Assert.AreEqual("1", result.Match["a"]);
            Assert.AreEqual("2", result.Match["b"]);
        }

        [TestMethod]
        public void CompareToDictionaryWithValueSelector()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty.Add("a", 10).Add("b", 20);
            var dict2 = ImmutableDictionary<string, int>.Empty.Add("a", 10).Add("b", 30);

            var result = dict1.CompareTo(dict2, v1 => v1 / 10, v2 => v2 / 10);

            Assert.AreEqual(1, result.Match.Count);
            Assert.AreEqual(1, result.Different.Count);
            Assert.AreEqual(2, result.Different["b"].Item1);
            Assert.AreEqual(3, result.Different["b"].Item2);
        }

        #endregion
    }
}
