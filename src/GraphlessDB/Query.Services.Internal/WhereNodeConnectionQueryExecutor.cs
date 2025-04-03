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
    internal sealed class WhereNodeConnectionQueryExecutor(IGraphCursorSerializationService cursorSerializer)
        : IGraphQueryNodeExecutionService<WhereNodeConnectionQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            var query = context.GetQuery<WhereNodeConnectionQuery>(key);
            var result = await ExecuteIterationAsync(context, key, query, cancellationToken);
            if (result != null)
            {
                context = context.SetResult(key, result);
            }

            return context;
        }

        private async Task<NodeConnectionResult> ExecuteIterationAsync(
            GraphExecutionContext context,
            string key,
            WhereNodeConnectionQuery query,
            CancellationToken cancellationToken)
        {
            var childResult = context.GetSingleChildResult<NodeConnectionResult>(key);
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

            pagedConnection = new Connection<RelayEdge<INode>, INode>(pagedConnection.Edges, newPageInfo);
            var needsMoreData = NeedsMoreData(query, childConnection, filteredConnection, pagedConnection);
            var hasMoreData = false;
            var cursor = childResult.Cursor;
            var childCursor = childResult.Cursor;
            return new NodeConnectionResult(
                childCursor,
                cursor,
                needsMoreData,
                hasMoreData,
                pagedConnection);
        }

        private Connection<RelayEdge<INode>, INode> WithSkippedInclusiveCursor(
            Connection<RelayEdge<INode>, INode> childConnection, string? cursorString)
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

            return new Connection<RelayEdge<INode>, INode>(
                edges,
                new PageInfo(
                    childConnection.PageInfo.HasNextPage,
                    childConnection.PageInfo.HasPreviousPage,
                    edges.TryGetStartCursor() ?? string.Empty,
                    edges.TryGetEndCursor() ?? string.Empty));
        }

        private static Connection<RelayEdge<INode>, INode> GetPagedResult(
            WhereNodeConnectionQuery query, Connection<RelayEdge<INode>, INode> connection)
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

        private async Task<Connection<RelayEdge<INode>, INode>> GetFilteredResultAsync(
            GraphExecutionContext context,
            WhereNodeConnectionQuery query,
            Connection<RelayEdge<INode>, INode> connection,
            CancellationToken cancellationToken)
        {
            var predicateResult = await Task.WhenAll(connection
                .Edges
                .Select(async edge => new
                {
                    edge = new RelayEdge<INode>(WithParentCursorNode(edge.Cursor, CursorNode.CreateEndOfData()), edge.Node),
                    match = await query.Predicate(new WhereRelayNodeContext<IGraph, INode>(
                        new FluentGraphQuery<IGraph>(context.GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        new FluentNodeQuery<IGraph, INode>(context.GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        edge, query.ConsistentRead, cancellationToken))
                }));

            var edges = predicateResult
                .Where(r => r.match)
                .Select(r => r.edge)
                .ToImmutableList();

            return new Connection<RelayEdge<INode>, INode>(
                edges,
                new PageInfo(
                    connection.Edges.Count > edges.Count || connection.PageInfo.HasNextPage,
                    connection.PageInfo.HasPreviousPage,
                    edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                    edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty));
        }

        private static bool NeedsMoreData(
            WhereNodeConnectionQuery query,
            Connection<RelayEdge<INode>, INode> childConnection,
            Connection<RelayEdge<INode>, INode> filteredConnection,
            Connection<RelayEdge<INode>, INode> pagedConnection)
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
            var result = context.GetResult<NodeConnectionResult>(key);
            var childResult = context.GetSingleChildResult<NodeConnectionResult>(key);
            return
                !childResult.Connection.Edges.IsEmpty &&
                result.ChildCursor != childResult.Connection.PageInfo.GetNullableEndCursor();
        }
    }
}
