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
using GraphlessDB.Collections;

namespace GraphlessDB.Query.Services
{
    public interface IGraphQueryExecutionService
    {
        Task<GraphExecutionContext> GetAsync(
            ImmutableTree<string, GraphQueryNode> query,
            CancellationToken cancellationToken);

        Task PutAsync(
            PutRequest request,
            CancellationToken cancellationToken);

        Task ClearAsync(
            CancellationToken cancellationToken);

        Task MutateAsync(
            Func<Task> operation,
            CancellationToken cancellationToken);

        Task<T> MutateAsync<T>(
            Func<Task<T>> operation,
            CancellationToken cancellationToken);
    }
}