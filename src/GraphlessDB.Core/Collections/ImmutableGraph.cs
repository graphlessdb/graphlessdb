/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;

namespace GraphlessDB.Collections
{
    public static class ImmutableGraph
    {
        public static ImmutableGraph<TNode, TEdge, TKey> Create<TNode, TEdge, TKey>(
            ImmutableList<TNode> nodes,
            Func<TNode, TKey> nodeKeySelector,
            ImmutableList<TEdge> edges,
            Func<TEdge, TKey> edgeInKeySelector,
            Func<TEdge, TKey> edgeOutKeySelector) where TKey : notnull
        {
            return new ImmutableGraph<TNode, TEdge, TKey>(
                ImmutableNodeList.Create(nodes, nodeKeySelector),
                ImmutableEdgeList.Create(edges, edgeInKeySelector, edgeOutKeySelector));
        }
    }
}
