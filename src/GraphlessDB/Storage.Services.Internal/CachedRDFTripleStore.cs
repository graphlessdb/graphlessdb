/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;

namespace GraphlessDB.Storage.Services.Internal
{
    internal sealed class CachedRDFTripleStore(
        IRDFTripleStore<StoreType.Data> rdfTripleStoreData,
        IMemoryCache memoryCache) : IRDFTripleStore<StoreType.Cached>
    {
        public async Task<GetRDFTriplesResponse> GetRDFTriplesAsync(
            GetRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            // Don't attempt to check for cached items if it is a ConsistentRead
            if (request.ConsistentRead)
            {
                var response = await rdfTripleStoreData.GetRDFTriplesAsync(request, cancellationToken);

                // Cache responses
                request
                    .Keys
                    .Select((key, index) => new RDFTripleCacheEntry(key, response.Items[index]))
                    .ToImmutableList()
                    .ForEach(v =>
                    {
                        memoryCache.AddOrUpdate(v.Key, v);
                    });

                return response;
            }

            var cacheEntriesByKey = request
                .Keys
                .Select(TryGet)
                .Where(r => r != null)
                .OfType<RDFTripleCacheEntry>()
                .ToImmutableDictionary(k => k.Key);

            var keysWithNoCachedValue = request
                .Keys
                .Where(key => !cacheEntriesByKey.ContainsKey(key))
                .ToImmutableList();

            var dataResponse = await rdfTripleStoreData.GetRDFTriplesAsync(
                request with { Keys = keysWithNoCachedValue }, cancellationToken);

            keysWithNoCachedValue
                .Select((key, i) => new { key, i })
                .ToImmutableList()
                .ForEach(v =>
                {
                    memoryCache.AddOrUpdate(v.key, new RDFTripleCacheEntry(v.key, dataResponse.Items[v.i]));
                });

            var nonCachedValuesByKey = dataResponse
                .Items
                .Where(k => k != null)
                .ToImmutableDictionary(k => k?.AsKey() ?? throw new GraphlessDBOperationException("Should not be possible"));

            return new GetRDFTriplesResponse(request
                .Keys
                .Select(k =>
                {
                    if (cacheEntriesByKey.TryGetValue(k, out var cachedValue))
                    {
                        return cachedValue.Value;
                    }

                    if (nonCachedValuesByKey.TryGetValue(k, out var value))
                    {
                        return value;
                    }

                    return null;
                })
                .ToImmutableList(),
                dataResponse.ConsumedCapacity);
        }

        public async Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(
            QueryRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            if (!request.ConsistentRead && !request.DisableInconsistentCacheRead && memoryCache.TryGetValue(request, out var response))
            {
                return (QueryRDFTriplesResponse?)response ?? throw new GraphlessDBOperationException("Response was null");
            }

            var dataResponse = await rdfTripleStoreData.QueryRDFTriplesAsync(request, cancellationToken);
            memoryCache.AddOrUpdate(request with { ConsistentRead = false }, dataResponse);
            return dataResponse;
        }

        public async Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(
            QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
        {
            if (!request.ConsistentRead && !request.DisableInconsistentCacheRead && memoryCache.TryGetValue(request, out var response))
            {
                return (QueryRDFTriplesResponse?)response ?? throw new GraphlessDBOperationException("Response was null");
            }

            var dataResponse = await rdfTripleStoreData.QueryRDFTriplesByPartitionAndPredicateAsync(request, cancellationToken);
            memoryCache.AddOrUpdate(request with { ConsistentRead = false }, dataResponse);
            return dataResponse;
        }

        public async Task RunHouseKeepingAsync(CancellationToken cancellationToken)
        {
            await rdfTripleStoreData.RunHouseKeepingAsync(cancellationToken);
        }

        public async Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(
            ScanRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            if (!request.ConsistentRead && !request.DisableInconsistentCacheRead && memoryCache.TryGetValue(request, out var response))
            {
                return (ScanRDFTriplesResponse?)response ?? throw new GraphlessDBOperationException("Response was null");
            }

            var dataResponse = await rdfTripleStoreData.ScanRDFTriplesAsync(request, cancellationToken);
            memoryCache.AddOrUpdate(request with { ConsistentRead = false }, dataResponse);
            return dataResponse;
        }

        public async Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(
            WriteRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            var response = await rdfTripleStoreData.WriteRDFTriplesAsync(request, cancellationToken);

            var keys = request
                .Items
                .Select(v => v.Add?.Item.AsKey() ?? v.CheckRDFTripleVersion?.Key ?? v.Delete?.Key ?? v.IncrementAllEdgesVersion?.Key ?? v.Update?.Item.AsKey() ?? v.UpdateAllEdgesVersion?.Key ?? throw new GraphlessDBOperationException())
                .ToImmutableList();

            keys.ForEach(key =>
            {
                memoryCache.TryRemove(key, out var _);
            });

            return response;
        }

        private RDFTripleCacheEntry? TryGet(RDFTripleKey key)
        {
            var hasValue = memoryCache.TryGetValue(key, out var value);
            if (!hasValue || value == null)
            {
                return null;
            }

            return (RDFTripleCacheEntry)value;
        }
    }
}
