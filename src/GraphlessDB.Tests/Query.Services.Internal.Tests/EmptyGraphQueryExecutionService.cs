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

namespace GraphlessDB.Query.Services.Internal.Tests
{
    public sealed class EmptyGraphQueryExecutionService : IGraphQueryExecutionService
    {
        public Task ClearAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<GraphExecutionContext> GetAsync(ImmutableTree<string, GraphQueryNode> query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task MutateAsync(Func<Task> operation, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<T> MutateAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
