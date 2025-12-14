/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Query.Services;

namespace GraphlessDB.Query.Internal
{
    internal interface IFromEdgeConnectionQueryExecutor
    {
        Task<GraphExecutionContext> ExecuteAsync(
           GraphExecutionContext context,
           string key,
           Func<IEdge, ImmutableList<string>> getTargetIds,
           CancellationToken cancellationToken);
    }
}
