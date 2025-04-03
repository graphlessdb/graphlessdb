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
    public sealed class SortableInt32StringTests
    {
        [TestMethod]
        public void CanStringSortNumbers()
        {
            var expected = new[] { int.MinValue, -1000, -500, -10, -5, 0, 5, 10, 500, 1000, int.MaxValue };

            var stringNumbers = expected
                .Select(SortableInt32String.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableInt32String.ToInt32)
                .ToArray();

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.AreEqual(expected[i], actual[i]);
            }
        }
    }
}