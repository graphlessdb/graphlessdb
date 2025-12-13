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
    public sealed record ImmutableNodeList<TNode, TKey>(
        ImmutableDictionary<TKey, TNode> ByKey) : IStructuralEquatable
        where TKey : notnull
    {
        public static readonly ImmutableNodeList<TNode, TKey> Empty = new(
            ImmutableDictionary<TKey, TNode>.Empty);

        public bool Equals(object? other, IEqualityComparer comparer)
        {
            return other switch
            {
                ImmutableNodeList<TNode, TKey> otherNodeList => comparer.Equals(ByKey, otherNodeList?.ByKey),
                _ => false,
            };
        }

        public int GetHashCode(IEqualityComparer comparer)
        {
            return comparer.GetHashCode(ByKey);
        }

        public bool Equals(ImmutableNodeList<TNode, TKey>? other)
        {
            return Equals(other, ImmutableStructuralEqualityComparer.Default);
        }

        public override int GetHashCode()
        {
            return GetHashCode(ImmutableStructuralEqualityComparer.Default);
        }
    }
}
