/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

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
    internal sealed class WhereEdgeConnectionQueryExecutor(IGraphCursorSerializationService cursorSerializer)
        : IGraphQueryNodeExecutionService<WhereEdgeConnectionQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            var query = context.GetQuery<WhereEdgeConnectionQuery>(key);
            var result = await ExecuteIterationAsync(context, key, query, cancellationToken);
            if (result != null)
            {
                context = context.SetResult(key, result);
            }

            return context;
        }

        private async Task<EdgeConnectionResult> ExecuteIterationAsync(
            GraphExecutionContext context,
            string key,
            WhereEdgeConnectionQuery query,
            CancellationToken cancellationToken)
        {
            var childResult = context.GetSingleChildResult<EdgeConnectionResult>(key);
            var childConnection = WithSkippedInclusiveCursor(childResult.Connection, query.Page.After);
            var filteredConnection = await GetFilteredResultAsync(context, query, childConnection, cancellationToken);
            var pagedConnection = context.Query.IsRootConnectionArgumentsKey(key)
                ? GetPagedResult(query, filteredConnection)
                : filteredConnection;

            var resultWasPaged = filteredConnection.Edges.Count > pagedConnection.Edges.Count;
            var newPageInfo = new PageInfo(
                resultWasPaged || childResult.Connection.PageInfo.HasNextPage,
                childResult.Connection.PageInfo.HasPreviousPage,
                pagedConnection.Edges.TryGetStartCursor() ?? string.Empty,
                pagedConnection.Edges.TryGetEndCursor() ?? string.Empty);

            pagedConnection = new Connection<RelayEdge<IEdge>, IEdge>(pagedConnection.Edges, newPageInfo);
            var needsMoreData = NeedsMoreData(query, childConnection, filteredConnection, pagedConnection);
            var hasMoreData = false;
            var cursor = childResult.Cursor;
            var childCursor = childResult.Cursor;
            return new EdgeConnectionResult(
                childCursor,
                cursor,
                needsMoreData,
                hasMoreData,
                pagedConnection);
        }

        private Connection<RelayEdge<IEdge>, IEdge> WithSkippedInclusiveCursor(
            Connection<RelayEdge<IEdge>, IEdge> childConnection, string? cursorString)
        {
            if (string.IsNullOrWhiteSpace(cursorString))
            {
                return childConnection;
            }

            var exclusiveChildStartCursor = TryGetChildCursor(cursorString);
            var edges = childConnection
                .Edges
                .SkipWhile(e => e.Cursor == exclusiveChildStartCursor)
                .ToImmutableList();

            return new Connection<RelayEdge<IEdge>, IEdge>(
                edges,
                new PageInfo(
                    childConnection.PageInfo.HasNextPage,
                    childConnection.PageInfo.HasPreviousPage,
                    edges.TryGetStartCursor() ?? string.Empty,
                    edges.TryGetEndCursor() ?? string.Empty));
        }

        private static Connection<RelayEdge<IEdge>, IEdge> GetPagedResult(
            WhereEdgeConnectionQuery query, Connection<RelayEdge<IEdge>, IEdge> connection)
        {
            var truncatedConnection = connection.Truncate(query.Page);
            if (truncatedConnection.Edges.Count < connection.Edges.Count)
            {
                return truncatedConnection with
                {
                    PageInfo = truncatedConnection.PageInfo with
                    {
                        HasNextPage = true
                    }
                };
            }

            return truncatedConnection;
        }

        private async Task<Connection<RelayEdge<IEdge>, IEdge>> GetFilteredResultAsync(
            GraphExecutionContext context,
            WhereEdgeConnectionQuery query,
            Connection<RelayEdge<IEdge>, IEdge> connection,
            CancellationToken cancellationToken)
        {
            var predicateResult = await Task.WhenAll(connection
                .Edges
                .Select(async edge => new
                {
                    edge = new RelayEdge<IEdge>(WithParentCursorNode(edge.Cursor, CursorNode.CreateEndOfData()), edge.Node),
                    match = await query.Predicate(new WhereRelayEdgeContext<IGraph, IEdge, INode, INode>(
                        new FluentGraphQuery<IGraph>(context.GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        new FluentEdgeQuery<IGraph, IEdge, INode, INode>(context.GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        edge, query.ConsistentRead, cancellationToken))
                }));

            var edges = predicateResult
                .Where(r => r.match)
                .Select(r => r.edge)
                .ToImmutableList();

            return new Connection<RelayEdge<IEdge>, IEdge>(
                edges,
                new PageInfo(
                    connection.Edges.Count > edges.Count || connection.PageInfo.HasNextPage,
                    connection.PageInfo.HasPreviousPage,
                    edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                    edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty));
        }

        private static bool NeedsMoreData(
            WhereEdgeConnectionQuery query,
            Connection<RelayEdge<IEdge>, IEdge> childConnection,
            Connection<RelayEdge<IEdge>, IEdge> filteredConnection,
            Connection<RelayEdge<IEdge>, IEdge> pagedConnection)
        {
            return childConnection.PageInfo.HasNextPage && query.Page.Count() > pagedConnection.Edges.Count;
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

        private string? TryGetChildCursor(Cursor cursor)
        {
            var childNodeKey = cursor.GetChildNodeKeys(cursor.GetRootKey()).SingleOrDefault();
            if (childNodeKey == null)
            {
                return null;
            }

            var childCursor = cursor.GetSubTree(childNodeKey);
            return TryGetCursor(childCursor);
        }

        private string? TryGetCursor(Cursor cursor)
        {
            if (cursor.Items.Nodes.ByKey.IsEmpty)
            {
                return null;
            }

            return cursorSerializer.Serialize(cursor);
        }

        public bool HasMoreChildData(GraphExecutionContext context, string key)
        {
            var result = context.GetResult<EdgeConnectionResult>(key);
            var childResult = context.GetSingleChildResult<EdgeConnectionResult>(key);
            return
                !childResult.Connection.Edges.IsEmpty &&
                result.ChildCursor != childResult.Connection.PageInfo.GetNullableEndCursor();
        }
    }
}
