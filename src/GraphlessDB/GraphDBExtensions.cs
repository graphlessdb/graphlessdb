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

namespace GraphlessDB
{
    public static class GraphDBExtensions
    {
        public static async Task MutateAsync(
            this IGraphDB source, Func<Task> operation, CancellationToken cancellationToken)
        {
            // TODO Perhaps add a way to ensure only one Put operation can be used
            await source
                .QueryExecutionService
                .MutateAsync(operation, cancellationToken);
        }

        public static async Task<T> MutateAsync<T>(
            this IGraphDB source, Func<Task<T>> operation, CancellationToken cancellationToken)
        {
            // TODO Perhaps add a way to ensure only one Put operation can be used
            return await source
                .QueryExecutionService
                .MutateAsync(operation, cancellationToken);
        }
    }
}