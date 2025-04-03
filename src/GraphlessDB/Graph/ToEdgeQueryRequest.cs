/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Graph
{
    public sealed record ToEdgeQueryRequest(
        string NodeTypeName,
        string? EdgeTypeName,
        Connection<RelayEdge<INode>, INode> NodeConnection,
        OrderArguments? OrderBy,
        EdgeFilterArguments? FilterBy,
        ConnectionArguments? ConnectionArguments,
        bool ConsistentRead);
}