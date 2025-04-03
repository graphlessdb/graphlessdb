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
    internal sealed class SingleOrDefaultEdgeQueryExecutor(IGraphCursorSerializationService cursorSerializer) : IGraphQueryNodeExecutionService<SingleOrDefaultEdgeQuery>
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

            if (connection.Edges.Count > 1)
            {
                // Too many results
                throw new GraphlessDBOperationException("Result contained more than a single edge");
            }

            if (connection.Edges.Count == 1 && !connection.PageInfo.HasNextPage)
            {
                // Great, single result with no more parent data to process
                var node = connection.Edges[0];
                return context.SetResult(key, new EdgeResult(
                    node.Cursor, node.Cursor, false, false, node));
            }

            if (connection.Edges.Count == 0 && !connection.PageInfo.HasNextPage)
            {
                // Great, no result with no more parent data to process
                return context.SetResult(key, new EdgeResult(
                    null, cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData())), false, false, null));
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