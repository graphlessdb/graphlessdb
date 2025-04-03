/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB
{
    public static class FluentPutExtensions
    {
        public static async Task ExecuteAsync(this FluentPut source, CancellationToken cancellationToken)
        {
            await source.GraphQueryService.PutAsync(
                new PutRequest(
                    MutationId.Create(Guid.NewGuid().ToString()),
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds,
                    source.Query.WithoutNodeEdgeChecks),
                cancellationToken);
        }

        public static async Task ExecuteAsync(this FluentPut source, MutationId mutationId, CancellationToken cancellationToken)
        {
            await source.GraphQueryService.PutAsync(
                new PutRequest(
                    mutationId,
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds,
                    source.Query.WithoutNodeEdgeChecks),
                cancellationToken);
        }

        public static FluentPut WithAllEdgesCheckForNodes(this FluentPut source, params INode[] nodes)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes.AddRange(nodes),
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds,
                    source.Query.WithoutNodeEdgeChecks));
        }
        public static FluentPut WithAllEdgesCheckForNodes(this FluentPut source, IEnumerable<INode> nodes)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes.AddRange(nodes),
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds,
                    source.Query.WithoutNodeEdgeChecks));
        }

        public static FluentPut WithEdgeByPropCheckForNodes(this FluentPut source, params EdgeByPropCheck[] values)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks.AddRange(values),
                    source.Query.NoEdgeChecksForNodeIds,
                    source.Query.WithoutNodeEdgeChecks));
        }

        public static FluentPut WithEdgeByPropCheckForNodes(this FluentPut source, IEnumerable<EdgeByPropCheck> values)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks.AddRange(values),
                    source.Query.NoEdgeChecksForNodeIds,
                    source.Query.WithoutNodeEdgeChecks));
        }

        public static FluentPut WithNoEdgesChecksForNodeIds(this FluentPut source, params string[] nodeIds)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds.AddRange(nodeIds),
                    source.Query.WithoutNodeEdgeChecks));
        }

        public static FluentPut WithNoEdgesChecksForNodeIds(this FluentPut source, IEnumerable<string> nodeIds)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds.AddRange(nodeIds),
                    source.Query.WithoutNodeEdgeChecks));
        }

        public static FluentPut WithNoEdgeChecksForAllNodes(this FluentPut source)
        {
            return new FluentPut(
                source.GraphQueryService,
                new PutQuery(
                    source.Query.PutEntities,
                    source.Query.AllEdgesCheckForNodes,
                    source.Query.EdgeByPropChecks,
                    source.Query.NoEdgeChecksForNodeIds,
                    true));
        }

    }
}
