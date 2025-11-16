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
    public sealed class ImmutableDictionarySequenceTests
    {
        [TestMethod]
        public void CanCreateWithEmptyDictionary()
        {
            var dict = ImmutableDictionary<string, int>.Empty;
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            Assert.IsNotNull(sequence);
            Assert.AreSame(dict, sequence.Items);
        }

        [TestMethod]
        public void CanCreateWithPopulatedDictionary()
        {
            var dict = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            Assert.IsNotNull(sequence);
            Assert.AreSame(dict, sequence.Items);
        }

        [TestMethod]
        public void EqualsReturnsTrueForSameSequence()
        {
            var dict = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            Assert.IsTrue(sequence.Equals(sequence));
        }

        [TestMethod]
        public void EqualsReturnsTrueForEqualSequences()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var dict2 = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var sequence1 = new ImmutableDictionarySequence<string, int>(dict1);
            var sequence2 = new ImmutableDictionarySequence<string, int>(dict2);

            Assert.IsTrue(sequence1.Equals(sequence2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentSequences()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var dict2 = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("three", 3);
            var sequence1 = new ImmutableDictionarySequence<string, int>(dict1);
            var sequence2 = new ImmutableDictionarySequence<string, int>(dict2);

            Assert.IsFalse(sequence1.Equals(sequence2));
        }

        [TestMethod]
        public void EqualsReturnsFalseForNull()
        {
            var dict = ImmutableDictionary<string, int>.Empty.Add("one", 1);
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            Assert.IsFalse(sequence.Equals(null));
        }

        [TestMethod]
        public void EqualsReturnsFalseForDifferentType()
        {
            var dict = ImmutableDictionary<string, int>.Empty.Add("one", 1);
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            Assert.IsFalse(sequence.Equals("string"));
        }

        [TestMethod]
        public void GetHashCodeReturnsSameValueForSameSequence()
        {
            var dict = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            var hash1 = sequence.GetHashCode();
            var hash2 = sequence.GetHashCode();

            Assert.AreEqual(hash1, hash2);
        }

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualSequences()
        {
            var dict1 = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var dict2 = ImmutableDictionary<string, int>.Empty
                .Add("one", 1)
                .Add("two", 2);
            var sequence1 = new ImmutableDictionarySequence<string, int>(dict1);
            var sequence2 = new ImmutableDictionarySequence<string, int>(dict2);

            Assert.AreEqual(sequence1.GetHashCode(), sequence2.GetHashCode());
        }

        [TestMethod]
        public void GetHashCodeHandlesNullValues()
        {
            var dict = ImmutableDictionary<string, string?>.Empty
                .Add("one", null)
                .Add("two", "value");
            var sequence = new ImmutableDictionarySequence<string, string?>(dict);

            var hash = sequence.GetHashCode();

            Assert.IsNotNull(hash);
        }

        [TestMethod]
        public void GetHashCodeHandlesEmptyDictionary()
        {
            var dict = ImmutableDictionary<string, int>.Empty;
            var sequence = new ImmutableDictionarySequence<string, int>(dict);

            var hash = sequence.GetHashCode();

            Assert.AreEqual(0, hash);
        }
    }
}
