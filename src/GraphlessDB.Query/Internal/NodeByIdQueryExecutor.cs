/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Domain;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Internal;
using GraphlessDB.Domain.Services;
using GraphlessDB.Query.Services;

namespace GraphlessDB.Query.Internal
{
    internal sealed class NodeByIdQueryExecutor(IGraphQueryService graphDataQueryService) : IGraphQueryNodeExecutionService<NodeByIdQuery>
    {
        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            return false;
        }

        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            var query = context.GetQuery<NodeByIdQuery>(key);

            var relayEdge = await graphDataQueryService
                .GetNodeAsync(query.Id, query.ConsistentRead, cancellationToken);

            var result = new NodeResult(
                null,
                relayEdge.Cursor,
                false,
                false,
                relayEdge);

            return context.SetResult(key, result);
        }
    }
}
