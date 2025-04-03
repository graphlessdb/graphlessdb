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
    internal sealed class OutFromEdgeQueryExecutor(
        IFromEdgeQueryExecutor fromEdgeQueryExecutor) : IGraphQueryNodeExecutionService<OutFromEdgeQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           CancellationToken cancellationToken)
        {
            return await fromEdgeQueryExecutor.ExecuteAsync(
                context,
                key,
                e => e.OutId,
                cancellationToken);
        }

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            var result = context.GetResult<NodeResult>(key);
            var childResult = context.GetSingleChildResult<EdgeResult>(key);
            return childResult.Edge != null &&
                result.Node == null;
        }

    }
}
