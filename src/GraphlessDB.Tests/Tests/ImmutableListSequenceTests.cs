/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Collections.Immutable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class ImmutableListSequenceTests
    {
        [TestMethod]
        public void EmptyReturnsEmptySequence()
        {
            var empty = ImmutableListSequence<int>.Empty;

            Assert.IsNotNull(empty);
            Assert.AreEqual(0, empty.Items.Count);
        }

        [TestMethod]
        public void ConstructorInitializesItemsProperty()
        {
            var items = ImmutableList.Create(1, 2, 3);
            var sequence = new ImmutableListSequence<int>(items);

            Assert.AreEqual(items, sequence.Items);
            Assert.AreEqual(3, sequence.Items.Count);
        }

        [TestMethod]
        public void GetHashCodeReturnsConsistentValue()
        {
            var items = ImmutableList.Create(1, 2, 3);
            var sequence = new ImmutableListSequence<int>(items);

            var hash1 = sequence.GetHashCode();
            var hash2 = sequence.GetHashCode();

            Assert.AreEqual(hash1, hash2);
        }

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualSequences()
        {
            var items1 = ImmutableList.Create(1, 2, 3);
            var sequence1 = new ImmutableListSequence<int>(items1);

            var items2 = ImmutableList.Create(1, 2, 3);
            var sequence2 = new ImmutableListSequence<int>(items2);

            Assert.AreEqual(sequence1.GetHashCode(), sequence2.GetHashCode());
        }

        [TestMethod]
        public void GetHashCodeHandlesNullItems()
        {
            var items = ImmutableList.Create<string?>("a", null, "b");
            var sequence = new ImmutableListSequence<string?>(items);

            var hash = sequence.GetHashCode();

            Assert.IsNotNull(hash);
        }

        [TestMethod]
        public void GetHashCodeForEmptySequence()
        {
            var empty = ImmutableListSequence<int>.Empty;

            var hash = empty.GetHashCode();

            Assert.AreEqual(0, hash);
        }

        [TestMethod]
        public void EqualsReturnsTrueForEqualSequences()
        {
            var items1 = ImmutableList.Create(1, 2, 3);
            var sequence1 = new ImmutableListSequence<int>(items1);

            var items2 = ImmutableList.Create(1, 2, 3);
            var sequence2 = new ImmutableListSequence<int>(items2);

            Assert.IsTrue(sequence1.Equals(sequence2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentSequences()
        {
            var items1 = ImmutableList.Create(1, 2, 3);
            var sequence1 = new ImmutableListSequence<int>(items1);

            var items2 = ImmutableList.Create(1, 2, 4);
            var sequence2 = new ImmutableListSequence<int>(items2);

            Assert.IsFalse(sequence1.Equals(sequence2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentLengthSequences()
        {
            var items1 = ImmutableList.Create(1, 2, 3);
            var sequence1 = new ImmutableListSequence<int>(items1);

            var items2 = ImmutableList.Create(1, 2);
            var sequence2 = new ImmutableListSequence<int>(items2);

            Assert.IsFalse(sequence1.Equals(sequence2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNonImmutableListSequenceObject()
        {
            var items = ImmutableList.Create(1, 2, 3);
            var sequence = new ImmutableListSequence<int>(items);

            Assert.IsFalse(sequence.Equals("not a sequence"));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNull()
        {
            var items = ImmutableList.Create(1, 2, 3);
            var sequence = new ImmutableListSequence<int>(items);

            Assert.IsFalse(sequence.Equals(null));
        }

        [TestMethod]
        public void EqualsReturnsTrueForEmptySequences()
        {
            var empty1 = ImmutableListSequence<int>.Empty;
            var empty2 = new ImmutableListSequence<int>(ImmutableList<int>.Empty);

            Assert.IsTrue(empty1.Equals(empty2));
        }

        [TestMethod]
        public void EqualsReturnsTrueForSameInstance()
        {
            var items = ImmutableList.Create(1, 2, 3);
            var sequence = new ImmutableListSequence<int>(items);

            Assert.IsTrue(sequence.Equals(sequence));
        }
    }
}
