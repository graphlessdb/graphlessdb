/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public sealed class InMemoryFullyAppliedRequestService : IFullyAppliedRequestService
    {
        private readonly Lock _lock;
        private ImmutableHashSet<TransactionVersion> _cache;

        public InMemoryFullyAppliedRequestService()
        {
            _lock = new Lock();
            _cache = [];
        }

        public async Task<bool> IsFullyAppliedAsync(TransactionVersion key, CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            return _cache.Contains(key);
        }

        public Task SetFullyAppliedAsync(TransactionVersion key, CancellationToken cancellationToken)
        {
            if (_cache.Contains(key))
            {
                return Task.CompletedTask;
            }

            lock (_lock)
            {
                if (!_cache.Contains(key))
                {
                    _cache = _cache.Add(key);
                }
            }

            return Task.CompletedTask;
        }
    }
}