/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Graph;

namespace GraphlessDB.Query.Services.Internal
{
    internal interface IToEdgeConnectionQueryExecutor
    {
        Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           Func<ToEdgeConnectionQuery, string> getQueryNodeSourceTypeName,
           Func<CursorNode, string> getCursorNodeInId,
           Func<CursorNode, string> getCursorNodeOutId,
           Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>> getToEdgeConnectionAsync,
           CancellationToken cancellationToken);
    }
}
