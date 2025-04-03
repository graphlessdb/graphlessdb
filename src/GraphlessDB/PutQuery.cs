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
    public sealed record PutQuery(
        ImmutableList<IEntity> PutEntities,
        ImmutableList<INode> AllEdgesCheckForNodes,
        ImmutableList<EdgeByPropCheck> EdgeByPropChecks,
        ImmutableList<string> NoEdgeChecksForNodeIds,
        bool WithoutNodeEdgeChecks) : GraphQuery
    {
        public static readonly PutQuery Empty = new([], [], [], [], false);
    }
}