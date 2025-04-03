/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB
{
    public sealed record Connection<TEdge, TNode>(ImmutableList<TEdge> Edges, PageInfo PageInfo)
        where TEdge : IRelayEdge<TNode>
    {
        public static readonly Connection<TEdge, TNode> Empty = new([], PageInfo.Empty);
    }
}