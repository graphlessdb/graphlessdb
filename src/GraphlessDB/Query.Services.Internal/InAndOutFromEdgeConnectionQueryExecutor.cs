/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class InAndOutFromEdgeConnectionQueryExecutor(
        IFromEdgeConnectionQueryExecutor fromEdgeConnectionQueryExecutor) : IGraphQueryNodeExecutionService<InAndOutFromEdgeConnectionQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            return await fromEdgeConnectionQueryExecutor
                .ExecuteAsync(context, key, edge => [edge.InId, edge.OutId], cancellationToken);
        }

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            var result = context.GetResult<NodeConnectionResult>(key);
            var childResult = context.GetSingleChildResult<EdgeConnectionResult>(key);
            return
                !childResult.Connection.Edges.IsEmpty &&
                result.ChildCursor != childResult.Connection.PageInfo.GetNullableEndCursor();
        }
    }
}
