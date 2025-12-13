/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Collections
{
    public sealed record ImmutableGraph<TNode, TEdge, TKey>(
        ImmutableNodeList<TNode, TKey> Nodes,
        ImmutableEdgeList<TEdge, TKey> Edges)
        where TKey : notnull
    {
        public static readonly ImmutableGraph<TNode, TEdge, TKey> Empty = new(
            ImmutableNodeList<TNode, TKey>.Empty,
            ImmutableEdgeList<TEdge, TKey>.Empty);
    }
}
