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
    public sealed class SortableDecimalStringTests
    {
        [TestMethod]
        public void CanStringSortNumbers()
        {
            var expected = new decimal[] { decimal.MinValue, -1000, -500, -10, -5, 0, 5, 10, 500, 1000, decimal.MaxValue };

            var stringNumbers = expected
                .Select(SortableDecimalString.ToString)
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToArray();

            var actual = stringNumbers
                .Select(SortableDecimalString.ToDecimal)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }

        [TestMethod]
        public void CanStringSortNumbers2()
        {
            var expected = new decimal[] {
                0.66m, 0.96m, 1.53m,
                1.99m, 2.70m,
                2.06m, 2.36m, 2.93m, 3.39m, 4.10m, 3.20m, 5.30m,
                8.99m, 20.25m, 28.55m, 2.66m, 2.95m, 4.40m,
                5.80m, 6.98m, 8.60m
                };

            expected = [.. expected.OrderBy(r => r)];

            var stringNumbers = expected
                .Select(SortableDecimalString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableDecimalString.ToDecimal)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }

        [TestMethod]
        public void CanStringSortNumbers3()
        {
            var expected = new decimal[] {
                0m,
                0.0005m,
                0.005m,
                0.05m,
                0.5m,
                5.0m,
                50.0m,
                500.0m,
                5000.0m,
                50000.0m
            };

            expected = [.. expected.OrderBy(r => r)];

            var stringNumbers = expected
                .Select(SortableDecimalString.ToString)
                .ToArray();

            var sortedStringNumbers = stringNumbers
                .ToArray();

            Array.Sort(sortedStringNumbers, StringComparer.Ordinal);

            var actual = sortedStringNumbers
                .Select(SortableDecimalString.ToDecimal)
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(actual));
        }
    }
}
