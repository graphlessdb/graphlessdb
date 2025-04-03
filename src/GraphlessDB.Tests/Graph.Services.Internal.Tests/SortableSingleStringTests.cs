/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class SortableSingleStringTests
    {
        [TestMethod]
        public void CanStringSortNumbers()
        {
            var expected = new float[] { float.MinValue, -1000, -500, -10, -5, 0, 5, 10, 500, 1000, float.MaxValue };

            var stringNumbers = expected
                .Select(SortableSingleString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableSingleString.ToSingle)
                .ToArray();

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.AreEqual(expected[i], actual[i]);
            }
        }


        [TestMethod]
        public void CanStringSortNumbers2()
        {
            var expected = new float[] {
                0.66f, 0.96f, 1.53f,
                1.99f, 2.70f,
                2.06f, 2.36f, 2.93f, 3.39f, 4.10f, 3.20f, 5.30f,
                8.99f, 20.25f, 28.55f, 2.66f, 2.95f, 4.40f,
                5.80f, 6.98f, 8.60f
                };

            expected = [.. expected.OrderBy(r => r)];

            var stringNumbers = expected
                .Select(SortableSingleString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableSingleString.ToSingle)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }

        [TestMethod]
        public void CanStringSortNumbers3()
        {
            var expected = new float[] {
                0f,
                0.0005f,
                0.005f,
                0.05f,
                0.5f,
                5.0f,
                50.0f,
                500.0f,
                5000.0f,
                50000.0f
            };

            expected = [.. expected.OrderBy(r => r)];

            var stringNumbers = expected
                .Select(SortableSingleString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableSingleString.ToSingle)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }
    }
}