/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Collections
{
    public sealed record ImmutableTree<TKey, TValue>(
        ImmutableNodeList<TValue, TKey> Nodes,
        ImmutableEdgeList<KeyPair<TKey>, TKey> Edges)
        where TKey : notnull
    {
        public static readonly ImmutableTree<TKey, TValue> Empty = new(
            ImmutableNodeList<TValue, TKey>.Empty,
            ImmutableEdgeList<KeyPair<TKey>, TKey>.Empty);
    }
}
