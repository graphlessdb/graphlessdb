/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;

namespace GraphlessDB
{
    public record WhereEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>(
        FluentGraphQuery<TGraph> Graph,
        FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut> FluentItem,
        TEdge Item,
        bool UseConsistentRead,
        CancellationToken CancellationToken)
        where TEdge : IEdge
        where TNodeIn : INode
        where TNodeOut : INode
        where TGraph : IGraph;
}
