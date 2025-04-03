/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class NodeConnectionQueryExecutor(
        IGraphQueryService graphDataQueryService,
        IGraphNodeFilterService graphQueryFiltering,
        IGraphCursorSerializationService cursorSerializer) : IGraphQueryNodeExecutionService<NodeConnectionQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            var isRootQuery = context.Query.GetRootKey() == key;
            var query = context.GetQuery<NodeConnectionQuery>(key);
            var result = context.TryGetResult<NodeConnectionResult>(key);
            while (ShouldExecute(result))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (CanRestoreIntermediateResultFromCursor(query, result, context.Query.IsRootConnectionArgumentsKey(key)))
                {
                    result = await RestoreIntermediateResultFromCursorAsync(query, cancellationToken);
                }

                result = await ExecuteIterationAsync(query, result, isRootQuery, cancellationToken);
            }

            if (result != null)
            {
                context = context.SetResult(key, result);
            }

            return context;
        }

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            // Node connection queries do not use child data
            return false;
        }

        private static bool CanRestoreIntermediateResultFromCursor(
            NodeConnectionQuery query,
            NodeConnectionResult? result,
            bool isRootQuery)
        {
            return result == null && !isRootQuery && query.Page.HasCursor();
        }

        private async Task<NodeConnectionResult> RestoreIntermediateResultFromCursorAsync(
            NodeConnectionQuery query,
            CancellationToken cancellationToken)
        {
            var cursorString = query.Page.Cursor();
            var cursor = cursorSerializer.Deserialize(cursorString);
            var cursorNode = cursor.GetRootNode();
            if (!cursor.GetChildNodeKeys(cursor.GetRootKey()).IsEmpty)
            {
                throw new GraphlessDBOperationException("Child queries not expected");
            }

            var id = cursorNode.HasType?.Subject ?? cursorNode.HasProp?.Subject ?? throw new GraphlessDBOperationException("Subject could not be found");
            var node = await graphDataQueryService.GetNodeAsync(id, query.ConsistentRead, cancellationToken);
            var edge = new RelayEdge<INode>(cursorString, node.Node);
            var connection = new Connection<RelayEdge<INode>, INode>(
                [edge],
                new PageInfo(true, false, edge.Cursor, edge.Cursor));

            return new NodeConnectionResult(
                null,
                cursorString,
                NeedsMoreData(query, connection),
                HasMoreData(connection),
                connection)
                .EnsureValid();
        }

        private async Task<NodeConnectionResult> ExecuteIterationAsync(
            NodeConnectionQuery query,
            NodeConnectionResult? result,
            bool isRootQuery,
            CancellationToken cancellationToken)
        {
            var iterationQuery = query with
            {
                Page = GetIterationPage(query, result)
            };

            var iterationConnection = await ExecuteGraphQueryAsync(
                iterationQuery, cancellationToken);

            // Now post process the results
            var filteredConnection = await graphQueryFiltering
                .GetFilteredNodeConnectionAsync(iterationConnection, query.Filter, query.ConsistentRead, cancellationToken);

            var existingConnection = result != null
                ? result.GetConnection<INode>()
                : Connection<RelayEdge<INode>, INode>.Empty;

            var resultConnection = existingConnection;
            var newEntities = resultConnection
                .Edges
                .AddRange(filteredConnection.Edges);

            var newPageInfo = new PageInfo(
                iterationConnection.PageInfo.HasNextPage,
                iterationConnection.PageInfo.HasPreviousPage,
                newEntities.TryGetStartCursor() ?? string.Empty,
                newEntities.TryGetEndCursor() ?? string.Empty);

            var deltaCount = newEntities.Count - existingConnection.Edges.Count;
            if (deltaCount > query.Page.Count())
            {
                // If we truncate the results then update the page info too
                newEntities = newEntities
                    .RemoveRange(query.Page.Count(), newEntities.Count - query.Page.Count());

                newPageInfo = new PageInfo(
                    true,
                    iterationConnection.PageInfo.HasPreviousPage,
                    newEntities.TryGetStartCursor() ?? string.Empty,
                    newEntities.TryGetEndCursor() ?? string.Empty);
            }

            resultConnection = new Connection<RelayEdge<INode>, INode>(newEntities, newPageInfo);
            var cursor = iterationConnection.PageInfo.GetNullableEndCursor() ?? cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData()));
            var nodeConnectionResult = new NodeConnectionResult(
                null,
                cursor,
                NeedsMoreData(query, resultConnection),
                HasMoreData(iterationConnection),
                resultConnection)
                .EnsureValid();

            if (isRootQuery && !ShouldExecute(nodeConnectionResult) && nodeConnectionResult.Connection.Edges.Count > query.Page.First)
            {
                // This is the final result in the tree and we are finished getting all the data we need
                // so now we should ensure the result is truncated to the correct page size in case we went over
                nodeConnectionResult = nodeConnectionResult with
                {
                    Connection = nodeConnectionResult.Connection.Truncate(query.Page)
                };
            }

            return nodeConnectionResult;
        }

        private async Task<Connection<RelayEdge<INode>, INode>> ExecuteGraphQueryAsync(
            NodeConnectionQuery query,
            CancellationToken cancellationToken)
        {
            var dataLayerNodeQuery = graphQueryFiltering
                .TryGetNodePushdownQueryData(query.Type, query.Filter, query.Order, cancellationToken);

            if (dataLayerNodeQuery != null && dataLayerNodeQuery.Filter != null && dataLayerNodeQuery.Filter.PropertyValues.Count > 1)
            {
                var filteredQuery = new GetConnectionByTypePropertyNameAndValuesRequest(
                        query.Type,
                        dataLayerNodeQuery.Order.PropertyName,
                        dataLayerNodeQuery.Filter.PropertyOperator,
                        dataLayerNodeQuery.Filter.PropertyValues,
                        dataLayerNodeQuery.Order.Direction == OrderDirection.Desc,
                        query.Page,
                        query.ConsistentRead);

                var filteredResponse = await graphDataQueryService
                    .GetConnectionByTypePropertyNameAndValuesAsync(filteredQuery, cancellationToken);

                return filteredResponse.Connection;
            }

            if (dataLayerNodeQuery != null && dataLayerNodeQuery.Filter != null && dataLayerNodeQuery.Filter.PropertyValues.Count == 1)
            {
                var filteredQuery = new GetConnectionByTypePropertyNameAndValueRequest(
                    query.Type,
                    dataLayerNodeQuery.Order.PropertyName,
                    dataLayerNodeQuery.Filter.PropertyOperator,
                    dataLayerNodeQuery.Filter.PropertyValues.Single(),
                    dataLayerNodeQuery.Order.Direction == OrderDirection.Desc,
                    query.Page,
                    query.ConsistentRead);

                var filteredResponse = await graphDataQueryService
                    .GetConnectionByTypePropertyNameAndValueAsync(filteredQuery, cancellationToken);

                return filteredResponse.Connection;
            }

            if (dataLayerNodeQuery != null)
            {
                var orderedQuery = new GetConnectionByTypeAndPropertyNameRequest(
                       query.Type,
                       dataLayerNodeQuery.Order.PropertyName,
                       dataLayerNodeQuery.Order.Direction == OrderDirection.Desc,
                       query.Page,
                       query.ConsistentRead);

                var orderedResponse = await graphDataQueryService
                    .GetConnectionByTypeAndPropertyNameAsync(orderedQuery, cancellationToken);

                return orderedResponse.Connection;
            }

            var queryRequest = new GetConnectionByTypeRequest(
                query.Type,
                query.Page,
                query.ConsistentRead);

            var queryResponse = await graphDataQueryService
                .GetConnectionByTypeAsync(queryRequest, cancellationToken);

            return queryResponse.Connection;
        }

        private static bool ShouldExecute(NodeConnectionResult? result)
        {
            return result == null || result.NeedsMoreData && result.HasMoreData;
        }

        private static bool NeedsMoreData(
            NodeConnectionQuery query,
            Connection<RelayEdge<INode>, INode> resultConnection)
        {
            return query.Page.Count() > resultConnection.Edges.Count;
        }

        private static bool HasMoreData(
            Connection<RelayEdge<INode>, INode> resultConnection)
        {
            return resultConnection.PageInfo.HasNextPage || resultConnection.PageInfo.HasPreviousPage;
        }

        private ConnectionArguments GetIterationPage(NodeConnectionQuery query, NodeConnectionResult? existingResult)
        {
            var isPostFilteringRequired = graphQueryFiltering
                .IsPostFilteringRequired(query.Filter);

            var first = isPostFilteringRequired
                ? query.PreFilteredPageSize
                : query.Page.First;

            var after = existingResult?.Cursor ?? query.Page.CursorOrDefault();

            return new ConnectionArguments(first, after, null, null);
        }
    }
}
