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
    internal sealed class FirstOrDefaultEdgeQueryExecutor(
        IGraphCursorSerializationService cursorSerializationService) : IGraphQueryNodeExecutionService<FirstOrDefaultEdgeQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            await Task.CompletedTask;

            var connection = context
                .GetSingleChildResult<EdgeConnectionResult>(key)
                .Connection;

            if (connection.Edges.Count >= 1)
            {
                // Great, have a result
                var node = connection.Edges[0];
                return context.SetResult(key, new EdgeResult(
                    node.Cursor, node.Cursor, false, false, node));
            }

            if (connection.Edges.Count == 0 && !connection.PageInfo.HasNextPage)
            {
                // Great, no result with no more parent data to process
                return context.SetResult(key, new EdgeResult(
                    null, cursorSerializationService.Serialize(Cursor.Create(CursorNode.CreateEndOfData())), false, false, null));
            }

            return context.SetResult(key, new EdgeResult(
                null, string.Empty, connection.PageInfo.HasNextPage, false, null));
        }

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            var result = context.GetResult<EdgeResult>(key);
            var childResult = context.GetSingleChildResult<EdgeConnectionResult>(key);
            return
                !childResult.Connection.Edges.IsEmpty &&
                result.ChildCursor != childResult.Connection.PageInfo.GetNullableEndCursor();
        }
    }
}
