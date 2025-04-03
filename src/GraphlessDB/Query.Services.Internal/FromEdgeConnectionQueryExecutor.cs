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
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class FromEdgeConnectionQueryExecutor(
        IGraphQueryService graphDataQueryService,
        IGraphNodeFilterService graphQueryFiltering,
        IGraphCursorSerializationService cursorSerializer) : IFromEdgeConnectionQueryExecutor
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           Func<IEdge, ImmutableList<string>> getTargetIds,
           CancellationToken cancellationToken)
        {
            var childResult = context.GetSingleChildResult<EdgeConnectionResult>(key);
            var childEdgeConnection = GetParentEdgeConnection(childResult);
            var query = context.GetQuery<FromEdgeConnectionQuery>(key);
            var result = context.TryGetResult<NodeConnectionResult>(key);
            var resultConnection = result != null
                ? result.GetConnection<INode>()
                : Connection<RelayEdge<INode>, INode>.Empty;

            var newNodes = await GetNodesAsync(query, result, childEdgeConnection, getTargetIds, cancellationToken);
            resultConnection = AppendEdgesToConnection(childEdgeConnection, resultConnection, newNodes);

            // NOTE There is no page into on this query so if this is final result
            // then we need to go to the child and use the connection args to truncate
            // a result which potentially has too much data
            var childQuery = context.GetSingleChildQuery(key);
            if (context.Query.GetRootKey() == key)
            {
                resultConnection = resultConnection.Truncate(GetPageInfo(childQuery));
            }

            var childCursor = newNodes.TryGetEndCursor() ?? childEdgeConnection.PageInfo.GetNullableEndCursor();
            var cursor = newNodes.TryGetEndCursor() ?? cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData()));

            result = new NodeConnectionResult(
                childCursor,
                cursor,
                NeedsMoreData(childQuery, resultConnection),
                HasMoreData(resultConnection),
                resultConnection)
                .EnsureValid();

            return context.SetResult(key, result);
        }

        private static Connection<RelayEdge<INode>, INode> AppendEdgesToConnection(
            Connection<RelayEdge<IEdge>, IEdge> childEdgeConnection,
            Connection<RelayEdge<INode>, INode> resultConnection,
            ImmutableList<RelayEdge<INode>> newNodes)
        {
            var newEntities = resultConnection
                .Edges
                .AddRange(newNodes)
                .GroupBy(v => v.Node.Id)
                .Select(v => v.Last())
                .ToImmutableList();

            var newPageInfo = new PageInfo(
                childEdgeConnection.PageInfo.HasNextPage,
                childEdgeConnection.PageInfo.HasPreviousPage,
                newEntities.TryGetStartCursor() ?? string.Empty,
                newEntities.TryGetEndCursor() ?? string.Empty);

            resultConnection = new Connection<RelayEdge<INode>, INode>(newEntities, newPageInfo);
            return resultConnection;
        }

        private static ConnectionArguments GetPageInfo(GraphQuery query)
        {
            return query switch
            {
                InToEdgeConnectionQuery q => q.Page,
                InToAllEdgeConnectionQuery q => q.Page,
                OutToEdgeConnectionQuery q => q.Page,
                OutToAllEdgeConnectionQuery q => q.Page,
                InAndOutToEdgeConnectionQuery q => q.Page,
                WhereEdgeConnectionQuery q => q.Page,
                // SingleEdgeQuery _ => 1,
                // SingleOrDefaultEdgeQuery _ => 1,
                // FirstEdgeQuery _ => 1,
                // FirstOrDefaultEdgeQuery _ => 1,
                _ => throw new NotSupportedException(),
            };
        }

        private static bool NeedsMoreData(
            GraphQuery childQuery,
            Connection<RelayEdge<INode>, INode> resultConnection)
        {
            var parentCount = childQuery switch
            {
                InToEdgeConnectionQuery q => q.Page.Count(),
                InToAllEdgeConnectionQuery q => q.Page.Count(),
                OutToEdgeConnectionQuery q => q.Page.Count(),
                OutToAllEdgeConnectionQuery q => q.Page.Count(),
                InAndOutToEdgeConnectionQuery q => q.Page.Count(),
                WhereEdgeConnectionQuery q => q.Page.Count(),
                SingleEdgeQuery => 1,
                SingleOrDefaultEdgeQuery => 1,
                FirstEdgeQuery => 1,
                FirstOrDefaultEdgeQuery => 1,
                _ => throw new NotSupportedException(),
            };

            return parentCount > resultConnection.Edges.Count;
        }

        private static bool HasMoreData(
            Connection<RelayEdge<INode>, INode> resultConnection)
        {
            return resultConnection.PageInfo.HasNextPage || resultConnection.PageInfo.HasPreviousPage;
        }

        private static Connection<RelayEdge<IEdge>, IEdge> GetParentEdgeConnection(GraphResult? parentResult)
        {
            var parentEdgeConnection = parentResult?.GetEdgeConnection<IEdge>() ?? throw new GraphlessDBOperationException("Parent edge connection was missing");
            var iterationEdges = parentEdgeConnection.Edges;
            return new Connection<RelayEdge<IEdge>, IEdge>(
                iterationEdges,
                new PageInfo(
                    false,
                    false,
                    iterationEdges.FirstOrDefault()?.Cursor ?? string.Empty,
                    iterationEdges.LastOrDefault()?.Cursor ?? string.Empty));
        }

        private async Task<ImmutableList<RelayEdge<INode>>> GetNodesAsync(
            FromEdgeConnectionQuery query,
            NodeConnectionResult? existingResult,
            Connection<RelayEdge<IEdge>, IEdge> parentEdgeConnection,
            Func<IEdge, ImmutableList<string>> getTargetIds,
            CancellationToken cancellationToken)
        {
            var childCursorsByNodeId = parentEdgeConnection
                .Edges
                .SelectMany(e => getTargetIds(e.Node).Select(id => new { id, edge = e }))
                .GroupBy(e => e.id)
                .ToImmutableDictionary(k => k.Key, v => v.Last().edge.Cursor);

            var edgeConnection = parentEdgeConnection;

            if (existingResult != null && existingResult.Cursor != null && existingResult.Cursor == null && !existingResult.HasMoreData)
            {
                throw new GraphlessDBOperationException();
            }

            if (existingResult != null && existingResult.ChildCursor != null && existingResult.Connection.PageInfo.HasNextPage)
            {
                edgeConnection = edgeConnection.FromCursorInclusive(existingResult.ChildCursor);
                // if (edgeConnection.Edges.Count == 0)
                // {
                //     throw new InvalidOperationException();
                // }

                // page = new ConnectionArguments(page.First, existingResult.Cursor, null, null);
            }

            if (existingResult != null && existingResult.ChildCursor != null && !existingResult.Connection.PageInfo.HasNextPage)
            {
                edgeConnection = edgeConnection.FromCursorExclusive(existingResult.ChildCursor);
                // if (edgeConnection.Edges.Count == 0)
                // {
                //     throw new InvalidOperationException();
                // }

                // page = new ConnectionArguments(page.First, existingResult.Cursor, null, null);
            }

            var targetNodeIds = edgeConnection
                .Edges
                .SelectMany(edge => getTargetIds(edge.Node))
                .Distinct()
                .ToImmutableList();

            // NOTE If there is a dangling edge then this will kill all expansions afterwards
            // so use try get and flag a warning
            var tryGetNodesRequest = new TryGetNodesRequest(targetNodeIds, query.ConsistentRead);
            var tryGetNodesResponse = await graphDataQueryService.TryGetNodesAsync(tryGetNodesRequest, cancellationToken);

            // var danglingEdgesIds = tryGetNodesResponse
            //     .Nodes
            //     .Select((node, i) => new { node, i })
            //     .Where(v => v.node == null)
            //     .Select(v => edgesWithDistinctDestinationNodeIds[v.i])
            //     .ToImmutableList();

            // foreach (var danglingEdgesId in danglingEdgesIds)
            // {
            //     _logger.LogWarning("Dangling edge detected. Id = {Id}", danglingEdgesId.Node.ToKey());
            // }

            var newNodes = tryGetNodesResponse
                .Nodes
                .Select((node, i) => new { node, i })
                .Where(v => v.node != null)
                .Select(v =>
                {
                    if (v.node == null)
                    {
                        throw new GraphlessDBOperationException("Expected non null node");
                    }

                    var edgeCursor = cursorSerializer.Deserialize(v.node.Cursor);
                    var childCursorString = childCursorsByNodeId[v.node.Node.Id];
                    // var childCursor = _cursorSerializer.Deserialize(childCursorString);
                    // var cursor = _cursorSerializer.Serialize(new Cursor(childCursor.Items));
                    var cursor = childCursorString;
                    return new RelayEdge<INode>(
                        cursor,
                        v.node?.Node ?? throw new InvalidOperationException("Expected non null node"));
                })
                .ToImmutableList();

            // return newNodes;

            var iterationConnection = new Connection<RelayEdge<INode>, INode>(
                newNodes,
                new PageInfo(
                    false,
                    false,
                    newNodes.FirstOrDefault()?.Cursor ?? string.Empty,
                    newNodes.LastOrDefault()?.Cursor ?? string.Empty));

            // Now post process the results
            var filteredConnection = await graphQueryFiltering
                .GetFilteredNodeConnectionAsync(iterationConnection, query.Filter, query.ConsistentRead, cancellationToken);

            var filteredNodes = filteredConnection.Edges;
            return filteredNodes;
        }
    }
}
