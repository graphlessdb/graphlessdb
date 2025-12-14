/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Services;
using GraphlessDB.Domain.Internal;
using GraphlessDB.Domain;
using GraphlessDB.Query.Services;
using GraphlessDB;

namespace GraphlessDB.Query.Internal
{
    internal sealed class NodeOrDefaultByIdQueryExecutor(
        IGraphQueryService graphDataQueryService,
        IGraphCursorSerializationService cursorSerializer) : IGraphQueryNodeExecutionService<NodeOrDefaultByIdQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            var query = context.GetQuery<NodeOrDefaultByIdQuery>(key);

            var relayEdge = await graphDataQueryService
                .TryGetNodeAsync(query.Id, query.ConsistentRead, cancellationToken);

            var result = new NodeResult(
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
