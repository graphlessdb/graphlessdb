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
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.Internal;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.Tests
{
    [TestClass]
    public sealed class RDFTripleStoreTests
    {
        private const string TableName = "TestTable";
        private sealed class MockRDFTripleStore : IRDFTripleStore<StoreType.Cached>
        {
            private readonly Func<GetRDFTriplesRequest, CancellationToken, Task<GetRDFTriplesResponse>>? _getRDFTriples;
            private readonly Func<QueryRDFTriplesRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? _queryRDFTriples;
            private readonly Func<QueryRDFTriplesByPartitionAndPredicateRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? _queryRDFTriplesByPartitionAndPredicate;
            private readonly Func<ScanRDFTriplesRequest, CancellationToken, Task<ScanRDFTriplesResponse>>? _scanRDFTriples;
            private readonly Func<WriteRDFTriplesRequest, CancellationToken, Task<WriteRDFTriplesResponse>>? _writeRDFTriples;
            private readonly Func<CancellationToken, Task>? _runHouseKeeping;

            public MockRDFTripleStore(
                Func<GetRDFTriplesRequest, CancellationToken, Task<GetRDFTriplesResponse>>? getRDFTriples = null,
                Func<QueryRDFTriplesRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? queryRDFTriples = null,
                Func<QueryRDFTriplesByPartitionAndPredicateRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? queryRDFTriplesByPartitionAndPredicate = null,
                Func<ScanRDFTriplesRequest, CancellationToken, Task<ScanRDFTriplesResponse>>? scanRDFTriples = null,
                Func<WriteRDFTriplesRequest, CancellationToken, Task<WriteRDFTriplesResponse>>? writeRDFTriples = null,
                Func<CancellationToken, Task>? runHouseKeeping = null)
            {
                _getRDFTriples = getRDFTriples;
                _queryRDFTriples = queryRDFTriples;
                _queryRDFTriplesByPartitionAndPredicate = queryRDFTriplesByPartitionAndPredicate;
                _scanRDFTriples = scanRDFTriples;
                _writeRDFTriples = writeRDFTriples;
                _runHouseKeeping = runHouseKeeping;
            }

            public Task<GetRDFTriplesResponse> GetRDFTriplesAsync(GetRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _getRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(QueryRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _queryRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
            {
                return _queryRDFTriplesByPartitionAndPredicate?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task RunHouseKeepingAsync(CancellationToken cancellationToken)
            {
                return _runHouseKeeping?.Invoke(cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(ScanRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _scanRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(WriteRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _writeRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }
        }

        private sealed class MockRDFTripleStoreData : IRDFTripleStore<StoreType.Data>
        {
            private readonly Func<GetRDFTriplesRequest, CancellationToken, Task<GetRDFTriplesResponse>>? _getRDFTriples;
            private readonly Func<QueryRDFTriplesRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? _queryRDFTriples;
            private readonly Func<QueryRDFTriplesByPartitionAndPredicateRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? _queryRDFTriplesByPartitionAndPredicate;
            private readonly Func<ScanRDFTriplesRequest, CancellationToken, Task<ScanRDFTriplesResponse>>? _scanRDFTriples;
            private readonly Func<WriteRDFTriplesRequest, CancellationToken, Task<WriteRDFTriplesResponse>>? _writeRDFTriples;
            private readonly Func<CancellationToken, Task>? _runHouseKeeping;

            public MockRDFTripleStoreData(
                Func<GetRDFTriplesRequest, CancellationToken, Task<GetRDFTriplesResponse>>? getRDFTriples = null,
                Func<QueryRDFTriplesRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? queryRDFTriples = null,
                Func<QueryRDFTriplesByPartitionAndPredicateRequest, CancellationToken, Task<QueryRDFTriplesResponse>>? queryRDFTriplesByPartitionAndPredicate = null,
                Func<ScanRDFTriplesRequest, CancellationToken, Task<ScanRDFTriplesResponse>>? scanRDFTriples = null,
                Func<WriteRDFTriplesRequest, CancellationToken, Task<WriteRDFTriplesResponse>>? writeRDFTriples = null,
                Func<CancellationToken, Task>? runHouseKeeping = null)
            {
                _getRDFTriples = getRDFTriples;
                _queryRDFTriples = queryRDFTriples;
                _queryRDFTriplesByPartitionAndPredicate = queryRDFTriplesByPartitionAndPredicate;
                _scanRDFTriples = scanRDFTriples;
                _writeRDFTriples = writeRDFTriples;
                _runHouseKeeping = runHouseKeeping;
            }

            public Task<GetRDFTriplesResponse> GetRDFTriplesAsync(GetRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _getRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(QueryRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _queryRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
            {
                return _queryRDFTriplesByPartitionAndPredicate?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task RunHouseKeepingAsync(CancellationToken cancellationToken)
            {
                return _runHouseKeeping?.Invoke(cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(ScanRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _scanRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }

            public Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(WriteRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                return _writeRDFTriples?.Invoke(request, cancellationToken)
                    ?? throw new NotImplementedException();
            }
        }

        private sealed class MockOptionsSnapshot : IOptionsSnapshot<RDFTripleStoreOptions>
        {
            public RDFTripleStoreOptions Value { get; set; }

            public MockOptionsSnapshot(RDFTripleStoreOptions options)
            {
                Value = options;
            }

            public RDFTripleStoreOptions Get(string? name)
            {
                return Value;
            }
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncRoutesToCachedStoreWhenCacheEnabled()
        {
            // Arrange
            var request = new GetRDFTriplesRequest(TableName, ImmutableList<RDFTripleKey>.Empty, false);
            var expectedResponse = new GetRDFTriplesResponse(ImmutableList<RDFTriple?>.Empty, null!);
            var cachedStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = true });
            var cachedStore = new MockRDFTripleStore(
                getRDFTriples: (req, ct) =>
                {
                    cachedStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });
            var dataStore = new MockRDFTripleStoreData();

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(cachedStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncRoutesToDataStoreWhenCacheDisabled()
        {
            // Arrange
            var request = new GetRDFTriplesRequest(TableName, ImmutableList<RDFTripleKey>.Empty, false);
            var expectedResponse = new GetRDFTriplesResponse(ImmutableList<RDFTriple?>.Empty, null!);
            var dataStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = false });
            var cachedStore = new MockRDFTripleStore();
            var dataStore = new MockRDFTripleStoreData(
                getRDFTriples: (req, ct) =>
                {
                    dataStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(dataStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncRoutesToCachedStoreWhenCacheEnabled()
        {
            // Arrange
            var request = new QueryRDFTriplesRequest(TableName, "partition", "predicate", null, false, 10, false, false);
            var expectedResponse = new QueryRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!);
            var cachedStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = true });
            var cachedStore = new MockRDFTripleStore(
                queryRDFTriples: (req, ct) =>
                {
                    cachedStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });
            var dataStore = new MockRDFTripleStoreData();

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.QueryRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(cachedStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncRoutesToDataStoreWhenCacheDisabled()
        {
            // Arrange
            var request = new QueryRDFTriplesRequest(TableName, "partition", "predicate", null, false, 10, false, false);
            var expectedResponse = new QueryRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!);
            var dataStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = false });
            var cachedStore = new MockRDFTripleStore();
            var dataStore = new MockRDFTripleStoreData(
                queryRDFTriples: (req, ct) =>
                {
                    dataStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.QueryRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(dataStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncRoutesToCachedStoreWhenCacheEnabled()
        {
            // Arrange
            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(TableName, "partition", "predicate", null, false, 10, false, false);
            var expectedResponse = new QueryRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!);
            var cachedStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = true });
            var cachedStore = new MockRDFTripleStore(
                queryRDFTriplesByPartitionAndPredicate: (req, ct) =>
                {
                    cachedStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });
            var dataStore = new MockRDFTripleStoreData();

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(cachedStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncRoutesToDataStoreWhenCacheDisabled()
        {
            // Arrange
            var request = new QueryRDFTriplesByPartitionAndPredicateRequest(TableName, "partition", "predicate", null, false, 10, false, false);
            var expectedResponse = new QueryRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!);
            var dataStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = false });
            var cachedStore = new MockRDFTripleStore();
            var dataStore = new MockRDFTripleStoreData(
                queryRDFTriplesByPartitionAndPredicate: (req, ct) =>
                {
                    dataStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.QueryRDFTriplesByPartitionAndPredicateAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(dataStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncRoutesToCachedStoreWhenCacheEnabled()
        {
            // Arrange
            var cachedStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = true });
            var cachedStore = new MockRDFTripleStore(
                runHouseKeeping: (ct) =>
                {
                    cachedStoreCalled = true;
                    return Task.CompletedTask;
                });
            var dataStore = new MockRDFTripleStoreData();

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            await store.RunHouseKeepingAsync(CancellationToken.None);

            // Assert
            Assert.IsTrue(cachedStoreCalled);
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncRoutesToDataStoreWhenCacheDisabled()
        {
            // Arrange
            var dataStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = false });
            var cachedStore = new MockRDFTripleStore();
            var dataStore = new MockRDFTripleStoreData(
                runHouseKeeping: (ct) =>
                {
                    dataStoreCalled = true;
                    return Task.CompletedTask;
                });

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            await store.RunHouseKeepingAsync(CancellationToken.None);

            // Assert
            Assert.IsTrue(dataStoreCalled);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncRoutesToCachedStoreWhenCacheEnabled()
        {
            // Arrange
            var request = new ScanRDFTriplesRequest(TableName, null, 10, false, false);
            var expectedResponse = new ScanRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!);
            var cachedStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = true });
            var cachedStore = new MockRDFTripleStore(
                scanRDFTriples: (req, ct) =>
                {
                    cachedStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });
            var dataStore = new MockRDFTripleStoreData();

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.ScanRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(cachedStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncRoutesToDataStoreWhenCacheDisabled()
        {
            // Arrange
            var request = new ScanRDFTriplesRequest(TableName, null, 10, false, false);
            var expectedResponse = new ScanRDFTriplesResponse(ImmutableList<RDFTriple>.Empty, false, null!);
            var dataStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = false });
            var cachedStore = new MockRDFTripleStore();
            var dataStore = new MockRDFTripleStoreData(
                scanRDFTriples: (req, ct) =>
                {
                    dataStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.ScanRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(dataStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncRoutesToCachedStoreWhenCacheEnabled()
        {
            // Arrange
            var request = new WriteRDFTriplesRequest("token", false, ImmutableList<WriteRDFTriple>.Empty);
            var expectedResponse = new WriteRDFTriplesResponse(null!);
            var cachedStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = true });
            var cachedStore = new MockRDFTripleStore(
                writeRDFTriples: (req, ct) =>
                {
                    cachedStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });
            var dataStore = new MockRDFTripleStoreData();

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(cachedStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }

        [TestMethod]
        public async Task WriteRDFTriplesAsyncRoutesToDataStoreWhenCacheDisabled()
        {
            // Arrange
            var request = new WriteRDFTriplesRequest("token", false, ImmutableList<WriteRDFTriple>.Empty);
            var expectedResponse = new WriteRDFTriplesResponse(null!);
            var dataStoreCalled = false;

            var options = new MockOptionsSnapshot(new RDFTripleStoreOptions { ScopeCacheEnabled = false });
            var cachedStore = new MockRDFTripleStore();
            var dataStore = new MockRDFTripleStoreData(
                writeRDFTriples: (req, ct) =>
                {
                    dataStoreCalled = true;
                    return Task.FromResult(expectedResponse);
                });

            var store = new RDFTripleStore(options, cachedStore, dataStore);

            // Act
            var result = await store.WriteRDFTriplesAsync(request, CancellationToken.None);

            // Assert
            Assert.IsTrue(dataStoreCalled);
            Assert.AreSame(expectedResponse, result);
        }
    }
}
