/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Linq;

namespace GraphlessDB.Collections.Immutable
{
    public sealed class ImmutableDictionarySequence<TKey, TValue>(ImmutableDictionary<TKey, TValue> items)
        where TKey : notnull
    {
        public ImmutableDictionary<TKey, TValue> Items { get; } = items;

        public override int GetHashCode()
        {
            unchecked
            {
                return Items.Aggregate(0, (agg, curr) => (agg * 397) ^ curr.Key.GetHashCode() * 397 ^ curr.Value?.GetHashCode() ?? 0);
            }
        }

        public override bool Equals(object? obj)
        {
            if (obj is ImmutableDictionarySequence<TKey, TValue> second)
            {
                return Items.SequenceEqual(second.Items);
            }

            return false;
        }
    }
}
