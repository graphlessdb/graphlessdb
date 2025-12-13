/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections;
using System.Collections.Immutable;

namespace GraphlessDB.Collections
{
    public sealed record ImmutableEdgeList<TEdge, TKey>(
        ImmutableDictionary<TKey, ImmutableList<TEdge>> ByInKey,
        ImmutableDictionary<TKey, ImmutableList<TEdge>> ByOutKey)
        : IStructuralEquatable
        where TKey : notnull
    {
        public static readonly ImmutableEdgeList<TEdge, TKey> Empty = new(
            ImmutableDictionary<TKey, ImmutableList<TEdge>>.Empty,
            ImmutableDictionary<TKey, ImmutableList<TEdge>>.Empty);

        public bool Equals(object? other, IEqualityComparer comparer)
        {
            return other switch
            {
                ImmutableEdgeList<TEdge, TKey> otherEdgeList => comparer.Equals(ByInKey, otherEdgeList?.ByInKey) && comparer.Equals(ByOutKey, otherEdgeList?.ByOutKey),
                _ => false,
            };
        }

        public int GetHashCode(IEqualityComparer comparer)
        {
            var hc = comparer.GetHashCode(ByInKey);
            hc ^= comparer.GetHashCode(ByOutKey);
            return hc;
        }

        public bool Equals(ImmutableEdgeList<TEdge, TKey>? other)
        {
            return Equals(other, ImmutableStructuralEqualityComparer.Default);
        }

        public override int GetHashCode()
        {
            return GetHashCode(ImmutableStructuralEqualityComparer.Default);
        }
    }
}