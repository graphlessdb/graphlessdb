/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services.Internal.FileBased;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.FileBased.Tests
{
    [TestClass]
    public sealed class FileBasedRDFTripleStoreTests
    {
        private const string TableName = "TestTable";
        private const string GraphName = "TestGraph";
        private string? _tempDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _tempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(_tempDirectory);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (_tempDirectory != null && Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, true);
            }
        }

        private FileBasedRDFTripleStore CreateStore(int partitionCount = 1)
        {
            var graphOptions = Options.Create(new GraphOptions
            {
                TableName = TableName,
                GraphName = GraphName,
                PartitionCount = partitionCount
            });

            var storageOptions = Options.Create(new FileBasedRDFTripleStoreOptions
            {
                StoragePath = _tempDirectory!
            });

            var eventReader = new FileBasedRDFEventReader(graphOptions);
            return new FileBasedRDFTripleStore(graphOptions, storageOptions, eventReader);
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
        public void ConstructorInitializesStorageDirectory()
        {
            var store = CreateStore();

            var dataDir = Path.Combine(_tempDirectory!, "data");
            var indexDir = Path.Combine(_tempDirectory!, "indexes", "by-predicate");

            Assert.IsTrue(Directory.Exists(dataDir));
            Assert.IsTrue(Directory.Exists(indexDir));
            Assert.IsTrue(File.Exists(Path.Combine(dataDir, "partition-0.jsonl")));
            Assert.IsTrue(File.Exists(Path.Combine(indexDir, "partition-0.jsonl")));
        }

        [TestMethod]
        public void ConstructorThrowsWhenEventReaderIsNull()
        {
            var graphOptions = Options.Create(new GraphOptions
            {
                TableName = TableName,
                GraphName = GraphName,
                PartitionCount = 1
            });

            var storageOptions = Options.Create(new FileBasedRDFTripleStoreOptions
            {
                StoragePath = _tempDirectory!
            });

            var exception = Assert.ThrowsException<ArgumentNullException>(() =>
            {
                var store = new FileBasedRDFTripleStore(graphOptions, storageOptions, null!);
            });

            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncReturnsEmptyForEmptyKeys()
        {
            var store = CreateStore();
            var request = new GetRDFTriplesRequest(TableName, ImmutableList<RDFTripleKey>.Empty, false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(0, response.Items.Count);
        }

        [TestMethod]
        public async Task GetRDFTriplesAsyncReturnsNullForNonExistentKey()
        {
            var store = CreateStore();
            var key = new RDFTripleKey("subject1", "predicate1");
            var request = new GetRDFTriplesRequest(TableName, [key], false);

            var response = await store.GetRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsNull(response.Items[0]);
        }

        [TestMethod]
        public async Task CanAddAndRetrieveRDFTriple()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var getRequest = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
            var response = await store.GetRDFTriplesAsync(getRequest, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.IsNotNull(response.Items[0]);
            Assert.AreEqual(triple.Subject, response.Items[0]!.Subject);
            Assert.AreEqual(triple.Predicate, response.Items[0]!.Predicate);
        }

        [TestMethod]
        public async Task AddRDFTripleThrowsWhenItemAlreadyExists()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task AddRDFTripleCreatesPredicateIndex()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var queryRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "0",
                "predicate",
                null,
                true,
                10,
                false,
                false);

            var response = await store.QueryRDFTriplesByPartitionAndPredicateAsync(queryRequest, CancellationToken.None);

            Assert.AreEqual(1, response.Items.Count);
            Assert.AreEqual(triple.Subject, response.Items[0].Subject);
        }

        [TestMethod]
        public async Task CanUpdateExistingRDFTriple()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 0));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var updatedTriple = triple with { Object = "updated", VersionDetail = new VersionDetail(2, 0) };
            var updateRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTriple(TableName, updatedTriple, new VersionDetailCondition(1, 0)))]);

            await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);

            var getRequest = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
            var response = await store.GetRDFTriplesAsync(getRequest, CancellationToken.None);

            Assert.AreEqual("updated", response.Items[0]!.Object);
            Assert.AreEqual(2, response.Items[0]!.VersionDetail!.NodeVersion);
        }

        [TestMethod]
        public async Task UpdateRDFTripleThrowsWhenItemDoesNotExist()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var updateRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTriple(TableName, triple, new VersionDetailCondition(null, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task UpdateRDFTripleThrowsWhenNodeVersionDoesNotMatch()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 0));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var updatedTriple = triple with { Object = "updated" };
            var updateRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTriple(TableName, updatedTriple, new VersionDetailCondition(2, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task UpdateRDFTripleThrowsWhenAllEdgesVersionDoesNotMatch()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 5));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var updatedTriple = triple with { Object = "updated" };
            var updateRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTriple(TableName, updatedTriple, new VersionDetailCondition(null, 10)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task CanDeleteExistingRDFTriple()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var deleteRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), new VersionDetailCondition(null, null)))]);

            await store.WriteRDFTriplesAsync(deleteRequest, CancellationToken.None);

            var getRequest = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
            var response = await store.GetRDFTriplesAsync(getRequest, CancellationToken.None);

            Assert.IsNull(response.Items[0]);
        }

        [TestMethod]
        public async Task DeleteRDFTripleRemovesFromPredicateIndex()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var deleteRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), new VersionDetailCondition(null, null)))]);

            await store.WriteRDFTriplesAsync(deleteRequest, CancellationToken.None);

            var queryRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "0",
                "predicate",
                null,
                true,
                10,
                false,
                false);

            var response = await store.QueryRDFTriplesByPartitionAndPredicateAsync(queryRequest, CancellationToken.None);

            Assert.AreEqual(0, response.Items.Count);
        }

        [TestMethod]
        public async Task DeleteRDFTripleThrowsWhenItemDoesNotExist()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1");
            var deleteRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), new VersionDetailCondition(null, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(deleteRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task DeleteRDFTripleThrowsWhenNodeVersionDoesNotMatch()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 0));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var deleteRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new DeleteRDFTriple(TableName, triple.AsKey(), new VersionDetailCondition(2, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(deleteRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncReturnsEmptyWhenSubjectDoesNotExist()
        {
            var store = CreateStore();
            var request = new QueryRDFTriplesRequest(
                TableName,
                "nonexistent",
                "predicate",
                null,
                true,
                10,
                false,
                false);

            var response = await store.QueryRDFTriplesAsync(request, CancellationToken.None);

            Assert.AreEqual(0, response.Items.Count);
            Assert.IsFalse(response.HasNextPage);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncReturnsMatchingTriples()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate:a");
            var triple2 = CreateTriple("subject1", "predicate:b");
            var triple3 = CreateTriple("subject1", "other:c");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple3))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var queryRequest = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate:",
                null,
                true,
                10,
                false,
                false);

            var response = await store.QueryRDFTriplesAsync(queryRequest, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
            Assert.AreEqual("predicate:a", response.Items[0].Predicate);
            Assert.AreEqual("predicate:b", response.Items[1].Predicate);
            Assert.IsFalse(response.HasNextPage);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncRespectsLimit()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate:a");
            var triple2 = CreateTriple("subject1", "predicate:b");
            var triple3 = CreateTriple("subject1", "predicate:c");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple3))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var queryRequest = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate:",
                null,
                true,
                2,
                false,
                false);

            var response = await store.QueryRDFTriplesAsync(queryRequest, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
            Assert.IsTrue(response.HasNextPage);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncSupportsPaginationForward()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate:a");
            var triple2 = CreateTriple("subject1", "predicate:b");
            var triple3 = CreateTriple("subject1", "predicate:c");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple3))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var queryRequest1 = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate:",
                null,
                true,
                2,
                false,
                false);

            var response1 = await store.QueryRDFTriplesAsync(queryRequest1, CancellationToken.None);

            Assert.AreEqual(2, response1.Items.Count);
            Assert.AreEqual("predicate:a", response1.Items[0].Predicate);
            Assert.AreEqual("predicate:b", response1.Items[1].Predicate);

            var queryRequest2 = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate:",
                new RDFTripleKey("subject1", "predicate:b"),
                true,
                10,
                false,
                false);

            var response2 = await store.QueryRDFTriplesAsync(queryRequest2, CancellationToken.None);

            Assert.AreEqual(1, response2.Items.Count);
            Assert.AreEqual("predicate:c", response2.Items[0].Predicate);
        }

        [TestMethod]
        public async Task QueryRDFTriplesAsyncSupportsPaginationBackward()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate:a");
            var triple2 = CreateTriple("subject1", "predicate:b");
            var triple3 = CreateTriple("subject1", "predicate:c");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple3))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var queryRequest = new QueryRDFTriplesRequest(
                TableName,
                "subject1",
                "predicate:",
                new RDFTripleKey("subject1", "predicate:c"),
                false,
                10,
                false,
                false);

            var response = await store.QueryRDFTriplesAsync(queryRequest, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
            Assert.AreEqual("predicate:b", response.Items[0].Predicate);
            Assert.AreEqual("predicate:a", response.Items[1].Predicate);
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncReturnsMatchingTriples()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate:a");
            var triple2 = CreateTriple("subject2", "predicate:b");
            var triple3 = CreateTriple("subject3", "other:c");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple3))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var queryRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "0",
                "predicate:",
                null,
                true,
                10,
                false,
                false);

            var response = await store.QueryRDFTriplesByPartitionAndPredicateAsync(queryRequest, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
            Assert.IsTrue(response.Items.Any(t => t.Subject == "subject1"));
            Assert.IsTrue(response.Items.Any(t => t.Subject == "subject2"));
        }

        [TestMethod]
        public async Task QueryRDFTriplesByPartitionAndPredicateAsyncThrowsWhenExclusiveStartKeyPartitionMismatch()
        {
            var store = CreateStore();

            var queryRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                TableName,
                "0",
                "predicate:",
                new RDFTripleKeyWithPartition("subject1", "predicate:a", "1"),
                true,
                10,
                false,
                false);

            await Assert.ThrowsExceptionAsync<ArgumentException>(async () =>
            {
                await store.QueryRDFTriplesByPartitionAndPredicateAsync(queryRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncReturnsAllTriples()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate1");
            var triple2 = CreateTriple("subject2", "predicate2");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var scanRequest = new ScanRDFTriplesRequest(TableName, null, 10, false, false);
            var response = await store.ScanRDFTriplesAsync(scanRequest, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
        }

        [TestMethod]
        public async Task ScanRDFTriplesAsyncRespectsLimit()
        {
            var store = CreateStore();
            var triple1 = CreateTriple("subject1", "predicate1");
            var triple2 = CreateTriple("subject2", "predicate2");
            var triple3 = CreateTriple("subject3", "predicate3");

            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple1)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple2)),
                    WriteRDFTriple.Create(new AddRDFTriple(TableName, triple3))
                ]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var scanRequest = new ScanRDFTriplesRequest(TableName, null, 2, false, false);
            var response = await store.ScanRDFTriplesAsync(scanRequest, CancellationToken.None);

            Assert.AreEqual(2, response.Items.Count);
            Assert.IsTrue(response.HasNextPage);
        }

        [TestMethod]
        public async Task UpdateAllEdgesVersionUpdatesVersion()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 5));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var updateRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTripleAllEdgesVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(1, 5),
                    10))]);

            await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);

            var getRequest = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
            var response = await store.GetRDFTriplesAsync(getRequest, CancellationToken.None);

            Assert.AreEqual(10, response.Items[0]!.VersionDetail!.AllEdgesVersion);
        }

        [TestMethod]
        public async Task UpdateAllEdgesVersionThrowsWhenItemDoesNotExist()
        {
            var store = CreateStore();
            var key = new RDFTripleKey("subject1", "predicate1");

            var updateRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTripleAllEdgesVersion(
                    TableName,
                    key,
                    new VersionDetailCondition(null, null),
                    10))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task UpdateAllEdgesVersionThrowsWhenVersionDetailIsNull()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: null);
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var updateRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new UpdateRDFTripleAllEdgesVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null),
                    10))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(updateRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task IncrementAllEdgesVersionIncrementsVersion()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 5));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var incrementRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new IncrementRDFTripleAllEdgesVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null)))]);

            await store.WriteRDFTriplesAsync(incrementRequest, CancellationToken.None);

            var getRequest = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
            var response = await store.GetRDFTriplesAsync(getRequest, CancellationToken.None);

            Assert.AreEqual(6, response.Items[0]!.VersionDetail!.AllEdgesVersion);
        }

        [TestMethod]
        public async Task IncrementAllEdgesVersionThrowsWhenItemDoesNotExist()
        {
            var store = CreateStore();
            var key = new RDFTripleKey("subject1", "predicate1");

            var incrementRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new IncrementRDFTripleAllEdgesVersion(
                    TableName,
                    key,
                    new VersionDetailCondition(null, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(incrementRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task IncrementAllEdgesVersionThrowsWhenVersionDetailIsNull()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: null);
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var incrementRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new IncrementRDFTripleAllEdgesVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(null, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(incrementRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task CheckRDFTripleVersionSucceedsWhenVersionMatches()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 5));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var checkRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new CheckRDFTripleVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(1, 5)))]);

            await store.WriteRDFTriplesAsync(checkRequest, CancellationToken.None);
        }

        [TestMethod]
        public async Task CheckRDFTripleVersionThrowsWhenItemDoesNotExist()
        {
            var store = CreateStore();
            var key = new RDFTripleKey("subject1", "predicate1");

            var checkRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new CheckRDFTripleVersion(
                    TableName,
                    key,
                    new VersionDetailCondition(null, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await store.WriteRDFTriplesAsync(checkRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task CheckRDFTripleVersionThrowsWhenVersionDoesNotMatch()
        {
            var store = CreateStore();
            var triple = CreateTriple("subject1", "predicate1", versionDetail: new VersionDetail(1, 5));
            var addRequest = new WriteRDFTriplesRequest(
                "token1",
                false,
                [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

            await store.WriteRDFTriplesAsync(addRequest, CancellationToken.None);

            var checkRequest = new WriteRDFTriplesRequest(
                "token2",
                false,
                [WriteRDFTriple.Create(new CheckRDFTripleVersion(
                    TableName,
                    triple.AsKey(),
                    new VersionDetailCondition(2, null)))]);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await store.WriteRDFTriplesAsync(checkRequest, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task RunHouseKeepingAsyncCompletes()
        {
            var store = CreateStore();

            await store.RunHouseKeepingAsync(CancellationToken.None);
        }

        [TestMethod]
        public async Task DataPersistedAcrossStoreInstances()
        {
            var triple = CreateTriple("subject1", "predicate1");

            {
                var store1 = CreateStore();
                var addRequest = new WriteRDFTriplesRequest(
                    "token1",
                    false,
                    [WriteRDFTriple.Create(new AddRDFTriple(TableName, triple))]);

                await store1.WriteRDFTriplesAsync(addRequest, CancellationToken.None);
            }

            {
                var store2 = CreateStore();
                var getRequest = new GetRDFTriplesRequest(TableName, [triple.AsKey()], false);
                var response = await store2.GetRDFTriplesAsync(getRequest, CancellationToken.None);

                Assert.AreEqual(1, response.Items.Count);
                Assert.IsNotNull(response.Items[0]);
                Assert.AreEqual(triple.Subject, response.Items[0]!.Subject);
            }
        }
    }
}
