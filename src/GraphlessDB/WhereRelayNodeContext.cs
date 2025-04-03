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
    public record WhereRelayNodeContext<TGraph, TNode>(
        FluentGraphQuery<TGraph> Graph,
        FluentNodeQuery<TGraph, TNode> FluentItem,
        RelayEdge<TNode> Item,
        bool UseConsistentRead,
        CancellationToken CancellationToken)
        where TNode : INode
        where TGraph : IGraph;
}
