/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class NodeByNodeQueryExecutor(IGraphQueryService graphDataQueryService) : IGraphQueryNodeExecutionService<NodeByNodeQuery>
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
            var query = context.GetQuery<NodeByNodeQuery>(key);

            var relayEdge = await graphDataQueryService
                .GetNodeAsync(query.Node.Id, query.ConsistentRead, cancellationToken);

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
