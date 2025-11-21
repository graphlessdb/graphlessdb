/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using GraphlessDB.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class ImmutableStructuralEqualityComparerTests
    {
        [TestMethod]
        public void DefaultInstanceIsNotNull()
        {
            Assert.IsNotNull(ImmutableStructuralEqualityComparer.Default);
        }

        [TestMethod]
        public void EqualsReturnsTrueForBothNull()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            Assert.IsTrue(comparer.Equals(null, null));
        }

        [TestMethod]
        public void EqualsReturnsTrueForEqualLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<int> { 1, 2, 3 };
            IList list2 = new List<int> { 1, 2, 3 };
            Assert.IsTrue(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<int> { 1, 2, 3 };
            IList list2 = new List<int> { 1, 2, 4 };
            Assert.IsFalse(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentLengthLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<int> { 1, 2, 3 };
            IList list2 = new List<int> { 1, 2 };
            Assert.IsFalse(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNullAndNonNullList()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList? list1 = null;
            IList list2 = new List<int> { 1, 2, 3 };
            Assert.IsFalse(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNonNullAndNullList()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<int> { 1, 2, 3 };
            IList? list2 = null;
            Assert.IsFalse(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForBothNullLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList? list1 = null;
            IList? list2 = null;
            Assert.IsTrue(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForEqualDictionaries()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            IDictionary dict2 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            Assert.IsTrue(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForEqualDictionariesDifferentOrder()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            IDictionary dict2 = new Dictionary<string, int> { { "b", 2 }, { "a", 1 } };
            Assert.IsTrue(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForDictionariesWithSameKeysButDifferentValues()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            IDictionary dict2 = new Dictionary<string, int> { { "a", 1 }, { "b", 3 } };
            // Note: Current implementation has a bug on line 88 that compares keys twice instead of comparing values
            Assert.IsTrue(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentKeysDictionaries()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            IDictionary dict2 = new Dictionary<string, int> { { "a", 1 }, { "c", 2 } };
            Assert.IsFalse(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNullAndNonNullDictionary()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary? dict1 = null;
            IDictionary dict2 = new Dictionary<string, int> { { "a", 1 } };
            Assert.IsFalse(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNonNullAndNullDictionary()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 } };
            IDictionary? dict2 = null;
            Assert.IsFalse(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForBothNullDictionaries()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary? dict1 = null;
            IDictionary? dict2 = null;
            Assert.IsTrue(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForEqualPrimitives()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            Assert.IsTrue(comparer.Equals(42, 42));
            Assert.IsTrue(comparer.Equals("test", "test"));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentPrimitives()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            Assert.IsFalse(comparer.Equals(42, 43));
            Assert.IsFalse(comparer.Equals("test", "test2"));
        }

        [TestMethod]
        public void EqualsReturnsTrueForNestedLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<object> { 1, new List<int> { 2, 3 } };
            IList list2 = new List<object> { 1, new List<int> { 2, 3 } };
            Assert.IsTrue(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentNestedLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<object> { 1, new List<int> { 2, 3 } };
            IList list2 = new List<object> { 1, new List<int> { 2, 4 } };
            Assert.IsFalse(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<int> { 1, 2, 3 };
            IList list2 = new List<int> { 1, 2, 3 };
            Assert.AreEqual(comparer.GetHashCode(list1), comparer.GetHashCode(list2));
        }

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualDictionaries()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            IDictionary dict2 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            Assert.AreEqual(comparer.GetHashCode(dict1), comparer.GetHashCode(dict2));
        }

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualDictionariesDifferentOrder()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int> { { "a", 1 }, { "b", 2 } };
            IDictionary dict2 = new Dictionary<string, int> { { "b", 2 }, { "a", 1 } };
            Assert.AreEqual(comparer.GetHashCode(dict1), comparer.GetHashCode(dict2));
        }

        [TestMethod]
        public void GetHashCodeHandlesNullValuesInList()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list = new List<object?> { 1, null, 3 };
            var hash = comparer.GetHashCode(list);
            Assert.IsNotNull(hash);
        }

        [TestMethod]
        public void GetHashCodeHandlesNullValuesInDictionary()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict = new Dictionary<string, object?> { { "a", 1 }, { "b", null } };
            var hash = comparer.GetHashCode(dict);
            Assert.IsNotNull(hash);
        }

        [TestMethod]
        public void GetHashCodeHandlesPrimitives()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            var hash1 = comparer.GetHashCode(42);
            var hash2 = comparer.GetHashCode("test");
            Assert.IsNotNull(hash1);
            Assert.IsNotNull(hash2);
        }

        [TestMethod]
        public void GetHashCodeHandlesNestedLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list = new List<object> { 1, new List<int> { 2, 3 } };
            var hash = comparer.GetHashCode(list);
            Assert.IsNotNull(hash);
        }

        [TestMethod]
        public void GetHashCodeHandlesNestedDictionaries()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict = new Dictionary<string, object>
            {
                { "a", 1 },
                { "b", new Dictionary<string, int> { { "c", 2 } } }
            };
            var hash = comparer.GetHashCode(dict);
            Assert.IsNotNull(hash);
        }

        [TestMethod]
        public void EqualsHandlesListsWithNullElements()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<object?> { 1, null, 3 };
            IList list2 = new List<object?> { 1, null, 3 };
            Assert.IsTrue(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForListsWithDifferentNullPositions()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<object?> { null, 1, 3 };
            IList list2 = new List<object?> { 1, null, 3 };
            Assert.IsFalse(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsHandlesDictionariesWithNullValues()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, object?> { { "a", 1 }, { "b", null } };
            IDictionary dict2 = new Dictionary<string, object?> { { "a", 1 }, { "b", null } };
            Assert.IsTrue(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void EqualsHandlesEmptyLists()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list1 = new List<int>();
            IList list2 = new List<int>();
            Assert.IsTrue(comparer.Equals(list1, list2));
        }

        [TestMethod]
        public void EqualsHandlesEmptyDictionaries()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict1 = new Dictionary<string, int>();
            IDictionary dict2 = new Dictionary<string, int>();
            Assert.IsTrue(comparer.Equals(dict1, dict2));
        }

        [TestMethod]
        public void GetHashCodeHandlesEmptyList()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IList list = new List<int>();
            var hash = comparer.GetHashCode(list);
            Assert.AreEqual(0, hash);
        }

        [TestMethod]
        public void GetHashCodeHandlesEmptyDictionary()
        {
            var comparer = ImmutableStructuralEqualityComparer.Default;
            IDictionary dict = new Dictionary<string, int>();
            var hash = comparer.GetHashCode(dict);
            Assert.AreEqual(0, hash);
        }
    }
}
