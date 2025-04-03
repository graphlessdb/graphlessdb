/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Logging;
using Microsoft.Extensions.Logging;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class FromEdgeQueryExecutor(
        IGraphQueryService graphDataQueryService,
        IGraphCursorSerializationService cursorSerializer,
        ILogger<FromEdgeQueryExecutor> logger) : IFromEdgeQueryExecutor
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           Func<IEdge, string> getTargetId,
           CancellationToken cancellationToken)
        {
            var query = context.GetQuery<FromEdgeQuery>(key);
            var childResult = context.GetSingleChildResult<EdgeResult>(key);
            var childEdgeConnection = GetParentEdgeConnection(childResult);
            var relayEdge = await TryGetNodeAsync(query, childEdgeConnection, getTargetId, cancellationToken);
            var result = new NodeResult(
                null,
                relayEdge?.Cursor ?? cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData())),
                NeedsMoreData(relayEdge),
                HasMoreData(),
                relayEdge);

            return context.SetResult(key, result);
        }

        private static bool NeedsMoreData(RelayEdge<INode>? resultNode)
        {
            return resultNode == null;
        }

        private static bool HasMoreData()
        {
            return false;
        }

        private static Connection<RelayEdge<IEdge>, IEdge> GetParentEdgeConnection(GraphResult? parentResult)
        {
            var parentEdgeConnection = parentResult?.GetEdgeConnection<IEdge>() ?? throw new GraphlessDBOperationException("Expected parent edge connection");
            var iterationEdges = parentEdgeConnection.Edges;
            return new Connection<RelayEdge<IEdge>, IEdge>(
                iterationEdges,
                new PageInfo(
                    false,
                    false,
                    iterationEdges.FirstOrDefault()?.Cursor ?? string.Empty,
                    iterationEdges.LastOrDefault()?.Cursor ?? string.Empty));
        }

        private async Task<RelayEdge<INode>?> TryGetNodeAsync(
            FromEdgeQuery query,
            Connection<RelayEdge<IEdge>, IEdge> parentEdgeConnection,
            Func<IEdge, string> getTargetId,
            CancellationToken cancellationToken)
        {
            var edgeConnection = parentEdgeConnection;

            var edgesWithDistinctDestinationNodeIds = edgeConnection
                .Edges
                .DistinctBy(e => getTargetId(e.Node))
                .ToImmutableList();

            var outNodeIds = edgesWithDistinctDestinationNodeIds
                .Select(edge => getTargetId(edge.Node))
                .Distinct()
                .ToImmutableList();

            // NOTE If there is a dangling edge then this will kill all expansions afterwards
            // so use try get and flag a warning
            var tryGetNodesRequest = new TryGetNodesRequest(outNodeIds, query.ConsistentRead);
            var tryGetNodesResponse = await graphDataQueryService.TryGetNodesAsync(tryGetNodesRequest, cancellationToken);

            var danglingEdgesIds = tryGetNodesResponse
                .Nodes
                .Select((node, i) => new { node, i })
                .Where(v => v.node == null)
                .Select(v => edgesWithDistinctDestinationNodeIds[v.i])
                .ToImmutableList();

            foreach (var danglingEdgesId in danglingEdgesIds)
            {
                logger.DanglingEdgeDetected(danglingEdgesId.Node.ToKey());
            }

            var newNodes = tryGetNodesResponse
                .Nodes
                .Select((node, i) => new { node, i })
                .Where(v => v.node != null)
                .Select(v => (RelayEdge<INode>?)new RelayEdge<INode>(
                    edgesWithDistinctDestinationNodeIds[v.i].Cursor,
                    v.node?.Node ?? throw new GraphlessDBOperationException("Expected non null node"))) // TODO Check cursor usage here
                .ToImmutableList();

            return newNodes.SingleOrDefault();
        }
    }
}
