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
    public sealed class SortableDoubleStringTests
    {
        [TestMethod]
        public void CanStringSortNumbers()
        {
            var expected = new double[] { double.MinValue, -1000, -500, -10, -5, 0, 5, 10, 500, 1000, double.MaxValue };

            var stringNumbers = expected
                .Select(SortableDoubleString.ToString)
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToArray();

            var actual = stringNumbers
                .Select(SortableDoubleString.ToDouble)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }

        [TestMethod]
        public void CanStringSortNumbers2()
        {
            var expected = new double[] {
                0.66, 0.96, 1.53,
                1.99, 2.70,
                2.06, 2.36, 2.93, 3.39, 4.10, 3.20, 5.30,
                8.99, 20.25, 28.55, 2.66, 2.95, 4.40,
                5.80, 6.98, 8.60
                };

            expected = [.. expected.OrderBy(r => r)];

            var stringNumbers = expected
                .Select(SortableDoubleString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableDoubleString.ToDouble)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }

        [TestMethod]
        public void CanStringSortNumbers3()
        {
            var expected = new double[] {
                0d,
                0.0005d,
                0.005d,
                0.05d,
                0.5d,
                5.0d,
                50.0d,
                500.0d,
                5000.0d,
                50000.0d
            };

            expected = [.. expected.OrderBy(r => r)];

            var stringNumbers = expected
                .Select(SortableDoubleString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableDoubleString.ToDouble)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }
    }
}
