/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;

namespace GraphlessDB.Collections.Immutable
{
    public sealed class ImmutableListSequence<T>(ImmutableList<T> items)
    {
        public static readonly ImmutableListSequence<T> Empty = new([]);

        public ImmutableList<T> Items { get; } = items;

        public override int GetHashCode()
        {
            unchecked
            {
                return Items.Aggregate(0, (agg, curr) => (agg * 397) ^ (curr != null ? curr.GetHashCode() : 0));
            }
        }

        public override bool Equals(object? obj)
        {
            if (obj is ImmutableListSequence<T> second)
            {
                return Items.SequenceEqual(second.Items);
            }

            return false;
        }
    }
}
