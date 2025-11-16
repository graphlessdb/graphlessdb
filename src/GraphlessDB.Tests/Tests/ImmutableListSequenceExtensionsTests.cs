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
using System.Linq;
using GraphlessDB.Collections.Immutable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class ImmutableListSequenceExtensionsTests
    {
        #region Add Single Item Tests

        [TestMethod]
        public void AddSingleItemAddsToEmptySequence()
        {
            var source = ImmutableListSequence<int>.Empty;
            var result = ImmutableListSequenceExtensions.Add(source, 1);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(1, result.Items[0]);
        }

        [TestMethod]
        public void AddSingleItemAddsToNonEmptySequence()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2));
            var result = ImmutableListSequenceExtensions.Add(source, 3);

            Assert.AreEqual(3, result.Items.Count);
            Assert.AreEqual(1, result.Items[0]);
            Assert.AreEqual(2, result.Items[1]);
            Assert.AreEqual(3, result.Items[2]);
        }

        [TestMethod]
        public void AddSingleItemDoesNotModifyOriginal()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1));
            var result = ImmutableListSequenceExtensions.Add(source, 2);

            Assert.AreEqual(1, source.Items.Count);
            Assert.AreEqual(2, result.Items.Count);
        }

        #endregion

        #region Add Multiple Items Tests

        [TestMethod]
        public void AddMultipleItemsAddsToEmptySequence()
        {
            var source = ImmutableListSequence<int>.Empty;
            var items = new List<int> { 1, 2, 3 };
            var result = ImmutableListSequenceExtensions.Add(source, items);

            Assert.AreEqual(3, result.Items.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, result.Items.ToList());
        }

        [TestMethod]
        public void AddMultipleItemsAddsToNonEmptySequence()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2));
            var items = new List<int> { 3, 4, 5 };
            var result = ImmutableListSequenceExtensions.Add(source, items);

            Assert.AreEqual(5, result.Items.Count);
            CollectionAssert.AreEqual(new List<int> { 1, 2, 3, 4, 5 }, result.Items.ToList());
        }

        [TestMethod]
        public void AddMultipleItemsHandlesEmptyCollection()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1));
            var items = new List<int>();
            var result = ImmutableListSequenceExtensions.Add(source, items);

            Assert.AreEqual(1, result.Items.Count);
            Assert.AreEqual(1, result.Items[0]);
        }

        [TestMethod]
        public void AddMultipleItemsDoesNotModifyOriginal()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1));
            var items = new List<int> { 2, 3 };
            var result = ImmutableListSequenceExtensions.Add(source, items);

            Assert.AreEqual(1, source.Items.Count);
            Assert.AreEqual(3, result.Items.Count);
        }

        #endregion

        #region SetItem Tests

        [TestMethod]
        public void SetItemReplacesItemAtIndex()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var result = ImmutableListSequenceExtensions.SetItem(source, 1, 99);

            Assert.AreEqual(3, result.Items.Count);
            Assert.AreEqual(1, result.Items[0]);
            Assert.AreEqual(99, result.Items[1]);
            Assert.AreEqual(3, result.Items[2]);
        }

        [TestMethod]
        public void SetItemAtFirstIndex()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var result = ImmutableListSequenceExtensions.SetItem(source, 0, 99);

            Assert.AreEqual(99, result.Items[0]);
            Assert.AreEqual(2, result.Items[1]);
            Assert.AreEqual(3, result.Items[2]);
        }

        [TestMethod]
        public void SetItemAtLastIndex()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var result = ImmutableListSequenceExtensions.SetItem(source, 2, 99);

            Assert.AreEqual(1, result.Items[0]);
            Assert.AreEqual(2, result.Items[1]);
            Assert.AreEqual(99, result.Items[2]);
        }

        [TestMethod]
        public void SetItemDoesNotModifyOriginal()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var result = ImmutableListSequenceExtensions.SetItem(source, 1, 99);

            Assert.AreEqual(2, source.Items[1]);
            Assert.AreEqual(99, result.Items[1]);
        }

        #endregion

        #region ReplaceSingle Tests

        [TestMethod]
        public void ReplaceSingleReplacesMatchingItem()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var result = ImmutableListSequenceExtensions.ReplaceSingle(source, x => x == 2, x => x * 10);

            Assert.AreEqual(3, result.Items.Count);
            Assert.AreEqual(1, result.Items[0]);
            Assert.AreEqual(20, result.Items[1]);
            Assert.AreEqual(3, result.Items[2]);
        }

        [TestMethod]
        public void ReplaceSingleUsesUpdaterFunction()
        {
            var source = new ImmutableListSequence<string>(ImmutableList<string>.Empty.Add("a").Add("b").Add("c"));
            var result = ImmutableListSequenceExtensions.ReplaceSingle(source, x => x == "b", x => x.ToUpperInvariant());

            Assert.AreEqual(3, result.Items.Count);
            Assert.AreEqual("a", result.Items[0]);
            Assert.AreEqual("B", result.Items[1]);
            Assert.AreEqual("c", result.Items[2]);
        }

        [TestMethod]
        public void ReplaceSingleDoesNotModifyOriginal()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var result = ImmutableListSequenceExtensions.ReplaceSingle(source, x => x == 2, x => x * 10);

            Assert.AreEqual(2, source.Items[1]);
            Assert.AreEqual(20, result.Items[1]);
        }

        #endregion

        #region IndexOf Tests

        [TestMethod]
        public void IndexOfReturnsCorrectIndex()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var index = ImmutableListSequenceExtensions.IndexOf(source, 2);

            Assert.AreEqual(1, index);
        }

        [TestMethod]
        public void IndexOfReturnsZeroForFirstItem()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var index = ImmutableListSequenceExtensions.IndexOf(source, 1);

            Assert.AreEqual(0, index);
        }

        [TestMethod]
        public void IndexOfReturnsLastIndexForLastItem()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var index = ImmutableListSequenceExtensions.IndexOf(source, 3);

            Assert.AreEqual(2, index);
        }

        [TestMethod]
        public void IndexOfReturnsMinusOneForNonExistentItem()
        {
            var source = new ImmutableListSequence<int>(ImmutableList<int>.Empty.Add(1).Add(2).Add(3));
            var index = ImmutableListSequenceExtensions.IndexOf(source, 99);

            Assert.AreEqual(-1, index);
        }

        [TestMethod]
        public void IndexOfReturnsMinusOneForEmptySequence()
        {
            var source = ImmutableListSequence<int>.Empty;
            var index = ImmutableListSequenceExtensions.IndexOf(source, 1);

            Assert.AreEqual(-1, index);
        }

        [TestMethod]
        public void IndexOfWorksWithReferenceTypes()
        {
            var source = new ImmutableListSequence<string>(ImmutableList<string>.Empty.Add("a").Add("b").Add("c"));
            var index = ImmutableListSequenceExtensions.IndexOf(source, "b");

            Assert.AreEqual(1, index);
        }

        #endregion
    }
}
