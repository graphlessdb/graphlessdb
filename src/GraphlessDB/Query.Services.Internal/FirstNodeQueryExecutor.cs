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
    internal sealed class FirstNodeQueryExecutor : IGraphQueryNodeExecutionService<FirstNodeQuery>
    {
        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            var result = context.GetResult<NodeResult>(key);
            var childResult = context.GetSingleChildResult<NodeConnectionResult>(key);
            return
                !childResult.Connection.Edges.IsEmpty &&
                result.ChildCursor != childResult.Connection.PageInfo.GetNullableEndCursor();
        }

        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            await Task.CompletedTask;

            var connection = context
                .GetSingleChildResult<NodeConnectionResult>(key)
                .Connection;

            if (connection.Edges.Count >= 1)
            {
                // Great, have result(s)
                var node = connection.Edges[0];
                return context.SetResult(key, new NodeResult(
                    node.Cursor, node.Cursor, false, false, node));
            }

            return context.SetResult(key, new NodeResult(
                null, string.Empty, connection.PageInfo.HasNextPage, false, null));
        }
    }
}

