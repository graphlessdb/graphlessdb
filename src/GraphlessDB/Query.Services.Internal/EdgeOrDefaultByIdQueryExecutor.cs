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
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class EdgeOrDefaultByIdQueryExecutor(
        IGraphQueryService graphDataQueryService,
        IGraphCursorSerializationService cursorSerializer) : IGraphQueryNodeExecutionService<EdgeOrDefaultByIdQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            var query = context.GetQuery<EdgeOrDefaultByIdQuery>(key);

            var edgeKey = new EdgeKey(query.EdgeTypeName, query.InId, query.OutId);

            var relayEdge = await graphDataQueryService
                .TryGetEdgeAsync(edgeKey, query.ConsistentRead, cancellationToken);

            var result = new EdgeResult(
                null,
                relayEdge?.Cursor ?? cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData())),
                false,
                false,
                relayEdge);

            return context.SetResult(key, result);
        }

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            return false;
        }
    }
}
