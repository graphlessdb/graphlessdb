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
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class ToEdgeConnectionQueryExecutor(
        IGraphCursorSerializationService cursorSerializer,
        IGraphQueryService graphDataQueryService,
        IGraphEdgeFilterService edgeFilterService) : IToEdgeConnectionQueryExecutor
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           Func<ToEdgeConnectionQuery, string> getQueryNodeSourceTypeName,
           Func<CursorNode, string> getCursorNodeInId,
           Func<CursorNode, string> getCursorNodeOutId,
           Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>> getToEdgeConnectionAsync,
           CancellationToken cancellationToken)
        {
            var query = context.GetQuery<ToEdgeConnectionQuery>(key);
            var result = context.TryGetResult<EdgeConnectionResult>(key);
            if (CanRestoreIntermediateResultFromCursor(query, result, context.Query.IsRootConnectionArgumentsKey(key)))
            {
                result = await RestoreIntermediateResultFromCursorAsync(query, getCursorNodeInId, getCursorNodeOutId, cancellationToken);
                if (result != null && !result.NeedsMoreData)
                {
                    context = context.SetResult(key, result);
                    return context;
                }
            }

            var childResult = context.GetSingleChildResult<GraphResult>(key);
            result = await ExecuteIterationAsync(query, result, childResult, getQueryNodeSourceTypeName, getToEdgeConnectionAsync, cancellationToken);
            if (result != null)
            {
                context = context.SetResult(key, result);
            }

            return context;
        }

        private async Task<EdgeConnectionResult> ExecuteIterationAsync(
            ToEdgeConnectionQuery query,
            EdgeConnectionResult? result,
            GraphResult childResult,
            Func<ToEdgeConnectionQuery, string> getQueryNodeSourceTypeName,
            Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>> getToEdgeConnectionAsync,
            CancellationToken cancellationToken)
        {
            var childResultConnection = childResult.GetConnection<INode>();
            var edgeKeyRequest = GetToEdgeQueryRequest(query, result, childResultConnection, getQueryNodeSourceTypeName);
            var edgeKeyResponse = await getToEdgeConnectionAsync(edgeKeyRequest, cancellationToken);

            // TODO How to handle a potential update between getting the edge ids and then getting the edges, retry on EdgesNotFoundException ???
            var iterationConnection = await GetEdgesAsync(
                childResultConnection, edgeKeyResponse.Connection, query.ConsistentRead, cancellationToken);

            // Now post process the results
            var filteredConnection = await edgeFilterService
                .GetFilteredEdgeConnectionAsync(iterationConnection, query.Filter, query.ConsistentRead, cancellationToken);

            var resultConnection = result != null
                ? result.GetEdgeConnection<IEdge>()
                : Connection<RelayEdge<IEdge>, IEdge>.Empty;

            var newEntities = resultConnection
                .Edges
                .AddRange(filteredConnection.Edges);

            if (newEntities.DistinctBy(n => n.Cursor).Count() != newEntities.Count)
            {
                throw new GraphlessDBOperationException("Unexpected duplicate edges");
            }

            var newPageInfo = new PageInfo(
                edgeKeyResponse.Connection.PageInfo.HasNextPage,
                edgeKeyResponse.Connection.PageInfo.HasPreviousPage,
                newEntities.TryGetStartCursor() ?? string.Empty,
                newEntities.TryGetEndCursor() ?? string.Empty);

            resultConnection = new Connection<RelayEdge<IEdge>, IEdge>(newEntities, newPageInfo);
            var needsMoreData = NeedsMoreData(query, resultConnection);
            var hasMoreData = !edgeKeyResponse.Connection.Edges.IsEmpty && HasMoreData(resultConnection);
            var cursor = iterationConnection.Edges.TryGetEndCursor() ?? WithParentCursorNode(childResult.Cursor, CursorNode.CreateEndOfData());
            var childCursor = TryGetChildCursor(cursor);
            return new EdgeConnectionResult(
                childCursor,
                cursor,
                needsMoreData,
                hasMoreData,
                resultConnection);
        }

        private static bool CanRestoreIntermediateResultFromCursor(ToEdgeConnectionQuery query, EdgeConnectionResult? result, bool isRootQuery)
        {
            return result == null && !isRootQuery && query.Page.HasCursor();
        }

        private async Task<EdgeConnectionResult> RestoreIntermediateResultFromCursorAsync(
            ToEdgeConnectionQuery query,
            Func<CursorNode, string> getCursorNodeInId,
            Func<CursorNode, string> getCursorNodeOutId,
            CancellationToken cancellationToken)
        {
            var pageCursor = query.Page.Cursor();
            var intermediateCursor = cursorSerializer.Deserialize(pageCursor);
            var toEdgeCursorItem = intermediateCursor.GetRootNode();
            if (toEdgeCursorItem.EndOfData != null)
            {
                // NOTE: In this case the cursor is saying that there was no data found
                // hence no real cursor but there is a flag to say end of data
                return new EdgeConnectionResult(
                    null,
                    pageCursor,
                    false,
                    false,
                    Connection<RelayEdge<IEdge>, IEdge>.Empty);
            }

            var nodeInId = getCursorNodeInId(toEdgeCursorItem);
            var nodeOutId = getCursorNodeOutId(toEdgeCursorItem);
            var edgeTypeName = query.EdgeTypeName ?? throw new NotSupportedException("Currently EdgeTypeName must be specified");
            var edge = await graphDataQueryService.GetEdgeAsync(new EdgeKey(edgeTypeName, nodeInId, nodeOutId), query.ConsistentRead, cancellationToken);
            var relayEdge = new RelayEdge<IEdge>(pageCursor, edge.Node);
            var resultConnection1 = ToConnection(relayEdge, true, false);
            var pageChildCursor = TryGetChildCursor(pageCursor);
            return new EdgeConnectionResult(
                pageChildCursor,
                pageCursor,
                NeedsMoreData(query, resultConnection1),
                HasMoreData(resultConnection1),
                resultConnection1);
        }

        private static Connection<RelayEdge<IEdge>, IEdge> ToConnection(RelayEdge<IEdge> edge, bool hasNextPage, bool hasPreviousPage)
        {
            return new Connection<RelayEdge<IEdge>, IEdge>(
                [edge],
                new PageInfo(hasNextPage, hasPreviousPage, edge.Cursor, edge.Cursor));
        }

        private string WithParentCursorNode(string childCursor, CursorNode cursorNode)
        {
            var cursor = cursorSerializer.Deserialize(childCursor).AddAsParentToRoot(cursorNode);
            return cursorSerializer.Serialize(cursor);
        }

        private string? TryGetChildCursor(string? cursor)
        {
            if (string.IsNullOrWhiteSpace(cursor))
            {
                return null;
            }

            return TryGetChildCursor(cursorSerializer.Deserialize(cursor));
        }

        private string? TryGetCursor(Cursor cursor)
        {
            if (cursor.Items.Nodes.ByKey.IsEmpty)
            {
                return null;
            }

            return cursorSerializer.Serialize(cursor);
        }

        private string? TryGetChildCursor(Cursor cursor)
        {
            // var childNode = cursor.GetChildNodes(cursor.GetRootNode()).SingleOrDefault();
            var childNodeKey = cursor.GetChildNodeKeys(cursor.GetRootKey()).SingleOrDefault();
            if (childNodeKey == null)
            {
                return null;
            }

            var childCursor = cursor.GetSubTree(childNodeKey);
            return TryGetCursor(childCursor);
        }

        private static bool NeedsMoreData(
            ToEdgeConnectionQuery query,
            Connection<RelayEdge<IEdge>, IEdge> resultConnection)
        {
            return query.Page.Count() > resultConnection.Edges.Count;
        }

        private static bool HasMoreData(
            Connection<RelayEdge<IEdge>, IEdge> resultConnection)
        {
            return resultConnection.PageInfo.HasNextPage || resultConnection.PageInfo.HasPreviousPage;
        }

        private async Task<Connection<RelayEdge<IEdge>, IEdge>> GetEdgesAsync(
            Connection<RelayEdge<INode>, INode> parentNodeConnection,
            Connection<RelayEdge<EdgeKey>, EdgeKey> edgeKeyConnection,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var childCursorsByNodeId = parentNodeConnection
                .Edges
                .GroupBy(e => e.Node.Id)
                .ToImmutableDictionary(k => k.Key, v => v.Last().Cursor);

            var getEdgesRequest = new GetEdgesRequest(edgeKeyConnection.Edges.Select(e => e.Node).ToImmutableList(), consistentRead);

            var getEdgesResponse = await graphDataQueryService.GetEdgesAsync(getEdgesRequest, cancellationToken);

            var edges = getEdgesResponse
                .Edges
                .Select((e, i) =>
                {
                    var childCursor = GetChildCursor(e, childCursorsByNodeId);
                    var cursor = edgeKeyConnection.Edges[i].Cursor;
                    var childCursorObj = cursorSerializer.Deserialize(childCursor);
                    var cursorObj = cursorSerializer.Deserialize(cursor);
                    var edgeCursorObj = childCursorObj.AddAsParentToRoot(cursorObj.GetRootNode());
                    var edgeCursor = cursorSerializer.Serialize(edgeCursorObj);
                    return new RelayEdge<IEdge>(edgeCursor, e.Node);
                })
                .ToImmutableList();

            return new Connection<RelayEdge<IEdge>, IEdge>(
                edges,
                new PageInfo(false, false, edges.TryGetStartCursor() ?? string.Empty, edges.TryGetEndCursor() ?? string.Empty));
        }

        private static string GetChildCursor(RelayEdge<IEdge> e, ImmutableDictionary<string, string> childCursorsByNodeId)
        {
            // NOTE: This is an expansion from an 'in' node to all edges but we cheat and do a
            // regular edge search, however, this search doesn't return a composite cursor so
            // we need to make one
            if (childCursorsByNodeId.TryGetValue(e.Node.InId, out var value))
            {
                return value;
            }

            return childCursorsByNodeId[e.Node.OutId];
        }

        private ToEdgeQueryRequest GetToEdgeQueryRequest(
            ToEdgeConnectionQuery query,
            EdgeConnectionResult? existingResult,
            Connection<RelayEdge<INode>, INode> parentNodeConnection,
            Func<ToEdgeConnectionQuery, string> getQueryNodeSourceTypeName)
        {
            var page = GetIterationPage(query);

            var nodeConnection = parentNodeConnection;

            if (existingResult != null && existingResult.Cursor != null)
            {
                // if (existingResult.Connection.PageInfo.HasNextPage)
                // {
                //     throw new GraphlessDBOperationException();
                // }

                var childCursor = TryGetChildCursor(existingResult.Cursor) ?? throw new GraphlessDBOperationException("Expected cursor");
                nodeConnection = nodeConnection.FromCursorInclusive(childCursor);
                if (nodeConnection.Edges.Count == 0)
                {
                    throw new GraphlessDBOperationException("Expected edges");
                }

                page = new ConnectionArguments(page.First, existingResult.Cursor, null, null);
            }
            else if (existingResult != null && existingResult.Cursor == null && existingResult.ChildCursor != null)
            {
                if (existingResult.Connection.PageInfo.HasNextPage)
                {
                    throw new GraphlessDBOperationException("Expected next page");
                }

                nodeConnection = nodeConnection.FromCursorExclusive(existingResult.ChildCursor);
                if (nodeConnection.Edges.Count == 0)
                {
                    throw new GraphlessDBOperationException("Expected edges");
                }

                page = new ConnectionArguments(page.First, null, null, null);
            }

            var edgePushdownQueryData = edgeFilterService.TryGetEdgePushdownQueryData(query.EdgeTypeName, query.Filter, query.Order);

            return new ToEdgeQueryRequest(
                getQueryNodeSourceTypeName(query),
                query.EdgeTypeName,
                nodeConnection,
                edgePushdownQueryData?.Order,
                edgePushdownQueryData?.Filter,
                page,
                query.ConsistentRead);
        }

        private ConnectionArguments GetIterationPage(ToEdgeConnectionQuery query)
        {
            var isPostFilteringRequired = edgeFilterService
                .IsPostFilteringRequired(query.Filter);

            return isPostFilteringRequired
                ? query.Page.WithCount(query.PreFilteredPageSize)
                : query.Page;
        }
    }
}
