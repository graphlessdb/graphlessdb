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
    public sealed class SortableInt64StringTests
    {
        [TestMethod]
        public void CanStringSortNumbers()
        {
            var expected = new long[] { long.MinValue, -1000, -500, -10, -5, 0, 5, 10, 500, 1000, long.MaxValue };

            var stringNumbers = expected
                .Select(SortableInt64String.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableInt64String.ToInt64)
                .ToArray();

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.AreEqual(expected[i], actual[i]);
            }
        }
    }
}