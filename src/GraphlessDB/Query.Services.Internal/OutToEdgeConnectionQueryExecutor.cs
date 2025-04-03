/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class OutToEdgeConnectionQueryExecutor(
        IGraphQueryService graphDataQueryService,
        IToEdgeConnectionQueryExecutor toEdgeConnectionQueryExecutor) : IGraphQueryNodeExecutionService<OutToEdgeConnectionQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            return await toEdgeConnectionQueryExecutor.ExecuteAsync(
                context,
                key,
                GetQueryNodeSourceTypeName,
                GetCursorNodeInId,
                GetCursorNodeOutId,
                graphDataQueryService.GetOutToEdgeConnectionAsync,
                cancellationToken);
        }

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            var result = context.GetResult<EdgeConnectionResult>(key);
            var childResult = context.GetSingleChildResult<GraphResult>(key).GetConnection<INode>();
            return !childResult.Edges.IsEmpty &&
                result.ChildCursor != childResult.PageInfo.GetNullableEndCursor();
        }

        private static string GetQueryNodeSourceTypeName(ToEdgeConnectionQuery query)
        {
            return query switch
            {
                OutToEdgeConnectionQuery outQuery => outQuery.NodeOutTypeName,
                _ => throw new GraphlessDBOperationException("Unexpected query type"),
            };
        }

        private static string GetCursorNodeInId(CursorNode value)
        {
            return value.HasOutEdge?.NodeInId ?? value.HasOutEdgeProp?.NodeInId ?? throw new GraphlessDBOperationException("Node in id was missing");
        }

        private static string GetCursorNodeOutId(CursorNode value)
        {
            return value.HasOutEdge?.Subject ?? value.HasOutEdgeProp?.Subject ?? throw new GraphlessDBOperationException("Node in id was missing");
        }
    }
}