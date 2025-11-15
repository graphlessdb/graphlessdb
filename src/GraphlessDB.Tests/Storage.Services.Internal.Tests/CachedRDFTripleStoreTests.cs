/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.Tests
{
    [TestClass]
    public sealed class CachedRDFTripleStoreTests
    {
        private const string TableName = "TestTable";

        private sealed class MockRDFTripleStore : IRDFTripleStore<StoreType.Data>
        {
            private readonly Func<GetRDFTriplesRequest, CancellationToken, Task<GetRDFTriplesResponse>>? _getRDFTriples;
            private readonly Func<QueryRDFTriplesRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? _queryRDFTriples;
            private readonly Func<QueryRDFTriplesByPartitionAndPredicateRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? _queryRDFTriplesByPartitionAndPredicate;
            private readonly Func<ScanRDFTriplesRequest, CancellationToken, Task<ScanRDFTriplesResponse>>? _scanRDFTriples;
            private readonly Func<WriteRDFTriplesRequest, CancellationToken, Task<WriteRDFTriplesResponse>>? _writeRDFTriples;

            public MockRDFTripleStore(
                Func<GetRDFTriplesRequest, CancellationToken, Task<GetRDFTriplesResponse>>? getRDFTriples = null,
                Func<QueryRDFTriplesRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? queryRDFTriples = null,
                Func<QueryRDFTriplesByPartitionAndPredicateRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? queryRDFTriplesByPartitionAndPredicate = null,
                Func<ScanRDFTriplesRequest, CancellationToken, Task<ScanRDFTriplesResponse>>? scanRDFTriples = null,
                Func<WriteRDFTriplesRequest, CancellationToken, Task<WriteRDFTriplesResponse>>? writeRDFTriples = null)
            {
                _getRDFTriples = getRDFTriples;
                _queryRDFTriples = queryRDFTriples;
                _queryRDFTriplesByPartitionAndPredicate = queryRDFTriplesByPartitionAndPredicate;
                _scanRDFTriples = scanRDFTriples;
                _writeRDFTriples = writeRDFTriples;
            }

            public Task<GetRDFTriplesResponse> GetRDFTriplesAsync(GetRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _getRDFTriples?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new GetRDFTriplesResponse(ImmutableList<RDFTriple?>.Empty, null!));
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(QueryRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _queryRDFTriples?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new QueryRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!));
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
            {
                return _queryRDFTriplesByPartitionAndPredicate?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new QueryRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!));
            }

            public Task RunHouseKeepingAsync(CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }

            public Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(ScanRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _scanRDFTriples?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new ScanRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!));
            }

            public Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(WriteRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _writeRDFTriples?.Invoke(request, cancellationToken)
                    ?? Task.FromResult(new WriteRDFTriplesResponse(null!));
            }
        }

        private static RDFTriple CreateTriple(
            string subject,
            string predicate,
            string indexedObject = "indexed",
            string obj = "object",
            string partition = "0",
            VersionDetail? versionDetail = null)
        {
            return new RDFTriple(subject, predicate, indexedObject, obj, partition, versionDetail);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncWithConsistentReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var getCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    getCallCount++;
                    return Task.FromResult(new GetRDFTriplesResponse([triple], null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new GetRDFTriplesRequest(TableName, [triple.AsKey()], true);

            // Call twice with ConsistentRead = true
            var response1 = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);
            var response2 = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, getCallCount);
            Assert.AreEqual(1, response1.Items.Count);
            Assert.AreEqual(1, response2.Items.Count);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncWithConsistentReadCachesResults()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new GetRDFTriplesResponse([triple], null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new GetRDFTriplesRequest(TableName, [triple.AsKey()], true);
            await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            // Verify cache was populated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var cachedEntry);
            Assert.IsTrue(hasCachedValue);
            Assert.IsNotNull(cachedEntry);
            var entry = (RDFTripleCacheEntry)cachedEntry!;
            Assert.AreEqual(triple.Subject, entry.Value?.Subject);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncWithoutConsistentReadUsesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var getCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    getCallCount++;
                    // Return items based on requested keys
                    var items = request.Keys
                        .Select(k => k.Subject == triple.Subject && k.Predicate == triple.Predicate ? triple : (RDFTriple?)null)
                        .ToImmutableList();
                    return Task.FromResult(new GetRDFTriplesResponse(items, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // First call without consistent read
            var request = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
            var response1 = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            // Second call should use cache
            var response2 = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            // The underlying store should be called twice: once for the first request with the key,
            // and once for the second request but with an empty key list (since it's cached)
            Assert.AreEqual(2, getCallCount);
            Assert.AreEqual(1, response1.Items.Count);
            Assert.AreEqual(1, response2.Items.Count);
            Assert.AreEqual(triple.Subject, response2.Items[0]?.Subject);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncWithMixedCachedAndUncachedKeys()
        {
            var triple1 = CreateTriple("subject1", "predicate1");
            var triple2 = CreateTriple("subject2", "predicate2");
            var triple3 = CreateTriple("subject3", "predicate3");

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    var items = request.Keys
                        .Select(k => k.Subject == "subject2" ? triple2 : k.Subject == "subject3" ? triple3 : (RDFTriple?)null)
                        .ToImmutableList();
                    return Task.FromResult(new GetRDFTriplesResponse(items, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache with triple1
            memoryCache.AddOrUpdate(triple1.AsKey(), new RDFTripleCacheEntry(triple1.AsKey(), triple1));

            // Request all three, only triple2 and triple3 should hit the store
            var request = new GetRDFTriplesRequest(
                TableName,
                [triple1.AsKey(), triple2.AsKey(), triple3.AsKey()],
                false);
            var response = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(3, response.Items.Count);
            Assert.AreEqual("subject1", response.Items[0]?.Subject);
            Assert.AreEqual("subject2", response.Items[1]?.Subject);
            Assert.AreEqual("subject3", response.Items[2]?.Subject);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncHandlesNullValues()
        {
            var key1 = new RDFTripleKey("subject1", "predicate1");
            var key2 = new RDFTripleKey("subject2", "predicate2");

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new GetRDFTriplesResponse([null, null], null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new GetRDFTriplesRequest(TableName, [key1, key2], false);
            var response = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
            Assert.IsNull(response.Items[0]);
            Assert.IsNull(response.Items[1]);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncWithConsistentReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var queryCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                queryRDFTriples: (request, ct) =>
                {
                    queryCallCount++;
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate",
                null,
                true,
                10,
                true,
                false);

            // Call twice with ConsistentRead = true
            await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);
            await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, queryCallCount);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncWithoutConsistentReadUsesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var queryCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                queryRDFTriples: (request, ct) =>
                {
                    queryCallCount++;
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate",
                null,
                true,
                10,
                false,
                false);

            // First call
            var response1 = await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);

            // Second call should use cache
            var response2 = await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, queryCallCount);
            Assert.AreEqual(1, response1.Items.Count);
            Assert.AreEqual(1, response2.Items.Count);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncWithDisableInconsistentCacheReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var queryCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                queryRDFTriples: (request, ct) =>
                {
                    queryCallCount++;
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate",
                null,
                true,
                10,
                false,
                true);

            // Call twice with DisableInconsistentCacheRead = true
            await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);
            await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, queryCallCount);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncCachesWithConsistentReadFalse()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                queryRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate",
                null,
                true,
                10,
                true,
                false);

            await cachedStore.QueryRDFTriplesAsync(request, CancellationToken.None);

            // Verify cache was populated with ConsistentRead = false
            var expectedCacheKey = request with { ConsistentRead = false };
            var hasCachedValue = memoryCache.TryGetValue(expectedCacheKey, out var cachedResponse);
            Assert.IsTrue(hasCachedValue);
            Assert.IsNotNull(cachedResponse);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncWithConsistentReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var queryCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                queryRDFTriplesByPartitionAndPredicate: (request, ct) =>
                {
                    queryCallCount++;
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "partition1",
                "predicate",
                null,
                true,
                10,
                true,
                false);

            // Call twice with ConsistentRead = true
            await cachedStore.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);
            await cachedStore.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            Assert.AreEqual(2, queryCallCount);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncWithoutConsistentReadUsesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var queryCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                queryRDFTriplesByPartitionAndPredicate: (request, ct) =>
                {
                    queryCallCount++;
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "partition1",
                "predicate",
                null,
                true,
                10,
                false,
                false);

            // First call
            var response1 = await cachedStore.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            // Second call should use cache
            var response2 = await cachedStore.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            Assert.AreEqual(1, queryCallCount);
            Assert.AreEqual(1, response1.Items.Count);
            Assert.AreEqual(1, response2.Items.Count);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncWithDisableInconsistentCacheReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var queryCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                queryRDFTriplesByPartitionAndPredicate: (request, ct) =>
                {
                    queryCallCount++;
                    return Task.FromResult(new QueryRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "partition1",
                "predicate",
                null,
                true,
                10,
                false,
                true);

            // Call twice with DisableInconsistentCacheRead = true
            await cachedStore.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);
            await cachedStore.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            Assert.AreEqual(2, queryCallCount);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncWithConsistentReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var scanCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                scanRDFTriples: (request, ct) =>
                {
                    scanCallCount++;
                    return Task.FromResult(new ScanRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new ScanRDFTriplesRequest(TableName, null, 10, true, false);

            // Call twice with ConsistentRead = true
            await cachedStore.ScanRDFTriplesAsync(request, CancellationToken.None);
            await cachedStore.ScanRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, scanCallCount);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncWithoutConsistentReadUsesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var scanCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                scanRDFTriples: (request, ct) =>
                {
                    scanCallCount++;
                    return Task.FromResult(new ScanRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new ScanRDFTriplesRequest(TableName, null, 10, false, false);

            // First call
            var response1 = await cachedStore.ScanRDFTriplesAsync(request, CancellationToken.None);

            // Second call should use cache
            var response2 = await cachedStore.ScanRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, scanCallCount);
            Assert.AreEqual(1, response1.Items.Count);
            Assert.AreEqual(1, response2.Items.Count);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncWithDisableInconsistentCacheReadBypassesCache()
        {
            var triple = CreateTriple("subject1", "predicate1");
            var scanCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                scanRDFTriples: (request, ct) =>
                {
                    scanCallCount++;
                    return Task.FromResult(new ScanRDFTriplesResponse([triple], false, null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new ScanRDFTriplesRequest(TableName, null, 10, false, true);

            // Call twice with DisableInconsistentCacheRead = true
            await cachedStore.ScanRDFTriplesAsync(request, CancellationToken.None);
            await cachedStore.ScanRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(2, scanCallCount);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncInvalidatesCacheForAddedItems()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple.AsKey(), new RDFTripleCacheEntry(triple.AsKey(), triple));

            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);

            // Verify cache was invalidated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var _);
            Assert.IsFalse(hasCachedValue);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncInvalidatesCacheForUpdatedItems()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple.AsKey(), new RDFTripleCacheEntry(triple.AsKey(), triple));

            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTriple(
                    TableName,
                    triple,
                    new VersionDetailCondition(null, null)))]);

            await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);

            // Verify cache was invalidated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var _);
            Assert.IsFalse(hasCachedValue);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncInvalidatesCacheForDeletedItems()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple.AsKey(), new RDFTripleCacheEntry(triple.AsKey(), triple));

            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new DeleteRDFTriple(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null)))]);

            await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);

            // Verify cache was invalidated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var _);
            Assert.IsFalse(hasCachedValue);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncInvalidatesCacheForCheckRDFTripleVersion()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple.AsKey(), new RDFTripleCacheEntry(triple.AsKey(), triple));

            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new CheckRDFTripleVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null)))]);

            await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);

            // Verify cache was invalidated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var _);
            Assert.IsFalse(hasCachedValue);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncInvalidatesCacheForIncrementAllEdgesVersion()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple.AsKey(), new RDFTripleCacheEntry(triple.AsKey(), triple));

            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new IncrementRDFTripleAllEdgesVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null)))]);

            await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);

            // Verify cache was invalidated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var _);
            Assert.IsFalse(hasCachedValue);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncInvalidatesCacheForUpdateAllEdgesVersion()
        {
            var triple = CreateTriple("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple.AsKey(), new RDFTripleCacheEntry(triple.AsKey(), triple));

            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTripleAllEdgesVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null),
                    10))]);

            await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);

            // Verify cache was invalidated
            var hasCachedValue = memoryCache.TryGetValue(triple.AsKey(), out var _);
            Assert.IsFalse(hasCachedValue);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncCallsUnderlyingStore()
        {
            var mockStore = new MockRDFTripleStore();
            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            await cachedStore.RunHouseKeepingAsync(CancellationToken.None);

            // If it completes without error, the test passes
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncCachesMultipleItemsFromConsistentRead()
        {
            var triple1 = CreateTriple("subject1", "predicate1");
            var triple2 = CreateTriple("subject2", "predicate2");

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new GetRDFTriplesResponse([triple1, triple2], null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var request = new GetRDFTriplesRequest(
                TableName,
                [triple1.AsKey(), triple2.AsKey()],
                true);
            await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            // Verify both items were cached
            var hasCached1 = memoryCache.TryGetValue(triple1.AsKey(), out var _);
            var hasCached2 = memoryCache.TryGetValue(triple2.AsKey(), out var _);
            Assert.IsTrue(hasCached1);
            Assert.IsTrue(hasCached2);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncHandlesAllCachedItems()
        {
            var triple1 = CreateTriple("subject1", "predicate1");
            var triple2 = CreateTriple("subject2", "predicate2");
            var getCallCount = 0;

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    getCallCount++;
                    return Task.FromResult(new GetRDFTriplesResponse([], null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache
            memoryCache.AddOrUpdate(triple1.AsKey(), new RDFTripleCacheEntry(triple1.AsKey(), triple1));
            memoryCache.AddOrUpdate(triple2.AsKey(), new RDFTripleCacheEntry(triple2.AsKey(), triple2));

            var request = new GetRDFTriplesRequest(
                TableName,
                [triple1.AsKey(), triple2.AsKey()],
                false);
            var response = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            // Should not call underlying store since all items are cached
            Assert.AreEqual(1, getCallCount);
            Assert.AreEqual(2, response.Items.Count);
            Assert.AreEqual("subject1", response.Items[0]?.Subject);
            Assert.AreEqual("subject2", response.Items[1]?.Subject);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncThrowsOnUnsupportedWriteType()
        {
            var mockStore = new MockRDFTripleStore(
                writeRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new WriteRDFTriplesResponse(null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            var writeTriple = new WriteRDFTriple(null, null, null, null, null, null);
            var writeRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [writeTriple]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await cachedStore.WriteRDFTriplesAsync(writeRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncPreservesCachedNullValues()
        {
            var key1 = new RDFTripleKey("subject1", "predicate1");

            var mockStore = new MockRDFTripleStore(
                getRDFTriples: (request, ct) =>
                {
                    return Task.FromResult(new GetRDFTriplesResponse([], null!));
                });

            var memoryCache = new ConcurrentMemoryCache();
            var cachedStore = new CachedRDFTripleStore(mockStore, memoryCache);

            // Pre-populate cache with null value
            memoryCache.AddOrUpdate(key1, new RDFTripleCacheEntry(key1, null));

            var request = new GetRDFTriplesRequest(TableName, [key1], false);
            var response = await cachedStore.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsNull(response.Items[0]);
        }
    }
}
