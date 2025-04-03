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
    internal interface IGraphQueryNodeExecutionService
    {
        bool HasMoreChildData(
            GraphExecutionContext context,
            string key);

        Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken);
    }

    internal interface IGraphQueryNodeExecutionService<T> : IGraphQueryNodeExecutionService
        where T : GraphQuery
    {

    }
}
