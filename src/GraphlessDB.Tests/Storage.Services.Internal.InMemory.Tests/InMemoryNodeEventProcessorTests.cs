/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Graph.Services;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services.Internal.InMemory;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.InMemory.Tests
{
    [TestClass]
    public sealed class InMemoryNodeEventProcessorTests
    {
        private const string TableName = "TestTable";
        private const string GraphName = "TestGraph";
        private const string TypeName = "TestType";

        private sealed class MockGraphSettingsService : IGraphSettingsService
        {
            private readonly GraphSettings _settings;

            public MockGraphSettingsService(GraphSettings settings)
            {
                _settings = settings;
            }

            public GraphSettings GetGraphSettings() => _settings;
        }

        private sealed class MockGraphEventService : IGraphEventService
        {
            public List<NodeEvent> NodeEvents { get; } = new List<NodeEvent>();
            public List<CancellationToken> CancellationTokens { get; } = new List<CancellationToken>();

            public Task OnNodeEventAsync(NodeEvent nodeEvent, CancellationToken cancellationToken)
            {
                NodeEvents.Add(nodeEvent);
                CancellationTokens.Add(cancellationToken);
                return Task.CompletedTask;
            }
        }

        private sealed class MockInMemoryRDFEventReader : IInMemoryRDFEventReader
        {
            private readonly Queue<ImmutableList<RDFTriple>> _queue;

            public MockInMemoryRDFEventReader(Queue<ImmutableList<RDFTriple>> queue)
            {
                _queue = queue;
            }

            public void OnRDFTripleAdded(RDFTriple value)
            {
                // Not used in these tests
            }

            public void OnRDFTripleUpdated(RDFTriple value)
            {
                // Not used in these tests
            }

            public ImmutableList<RDFTriple> DequeueRDFTripleEvents()
            {
                return _queue.Count > 0 ? _queue.Dequeue() : ImmutableList<RDFTriple>.Empty;
            }
        }

        private sealed class MockRDFTripleFactory : IRDFTripleFactory
        {
            private readonly Dictionary<RDFTriple, INode> _nodes = new Dictionary<RDFTriple, INode>();

            public void AddNode(RDFTriple triple, INode node)
            {
                _nodes[triple] = node;
            }

            public INode GetNode(RDFTriple rdfTriple)
            {
                if (_nodes.TryGetValue(rdfTriple, out var node))
                {
                    return node;
                }

                // Try to find by key properties for old nodes (version might differ)
                foreach (var kvp in _nodes)
                {
                    if (kvp.Key.Subject == rdfTriple.Subject && kvp.Key.Predicate == rdfTriple.Predicate)
                    {
                        return kvp.Value;
                    }
                }

                throw new InvalidOperationException($"Node not found for triple: {rdfTriple.Subject}, {rdfTriple.Predicate}");
            }

            public IEdge GetEdge(RDFTriple item) => throw new NotImplementedException();
            public RDFTriple HasType(INode node) => throw new NotImplementedException();
            public RDFTriple HasBlob(INode node) => throw new NotImplementedException();
            public RDFTriple HasProp(INode node, string propertyName, string propertyValue) => throw new NotImplementedException();
            public RDFTriple HasInEdge(IEdge edge) => throw new NotImplementedException();
            public RDFTriple HasInEdgeProp(IEdge edge, string propertyName, string propertyValue) => throw new NotImplementedException();
            public RDFTriple HasOutEdge(IEdge edge) => throw new NotImplementedException();
            public RDFTriple HasOutEdgeProp(IEdge edge, string propertyName, string propertyValue) => throw new NotImplementedException();
            public RDFTriple HadInEdge(IEdge edge) => throw new NotImplementedException();
            public RDFTriple HadOutEdge(IEdge edge) => throw new NotImplementedException();
            public RDFTriple GetHasTypeRDFTriple(INode node) => throw new NotImplementedException();
            public RDFTriple GetHasBlobRDFTriple(INode node) => throw new NotImplementedException();
            public ImmutableList<RDFTriple> GetHasEdgeRDFTriples(IEdge edge) => throw new NotImplementedException();
            public ImmutableList<RDFTriple> GetHasEdgePropRDFTriples(IEdge edge) => throw new NotImplementedException();
            public ImmutableList<RDFTriple> GetHadEdgeRDFTriples(IEdge edge) => throw new NotImplementedException();
            public ImmutableList<RDFTriple> GetHasPropRDFTriples(INode node) => throw new NotImplementedException();
        }

        private sealed class MockRDFTripleStore : IRDFTripleStore
        {
            private readonly Dictionary<string, GetRDFTriplesResponse> _responses = new Dictionary<string, GetRDFTriplesResponse>();
            public List<GetRDFTriplesRequest> GetRequests { get; } = new List<GetRDFTriplesRequest>();
            public List<CancellationToken> GetCancellationTokens { get; } = new List<CancellationToken>();

            public void AddResponse(string key, GetRDFTriplesResponse response)
            {
                _responses[key] = response;
            }

            public Task<GetRDFTriplesResponse> GetRDFTriplesAsync(GetRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                GetRequests.Add(request);
                GetCancellationTokens.Add(cancellationToken);

                if (request.Keys.Count > 0)
                {
                    var key = request.Keys[0].Predicate;
                    if (_responses.TryGetValue(key, out var response))
                    {
                        return Task.FromResult(response);
                    }
                }

                return Task.FromResult(new GetRDFTriplesResponse(ImmutableList<RDFTriple?>.Empty, RDFTripleStoreConsumedCapacity.None()));
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(QueryRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(ScanRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(WriteRDFTriplesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task RunHouseKeepingAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed record MockNode(string Id, VersionDetail Version, DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt)
            : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
        {
            public MockNode(string id) : this(id, new VersionDetail(0, 0), DateTime.UtcNow, DateTime.UtcNow, DateTime.MaxValue)
            {
            }
        }

        private static InMemoryNodeEventProcessor CreateProcessor(
            MockGraphSettingsService graphSettingsService,
            MockGraphEventService graphEventService,
            MockInMemoryRDFEventReader rdfEventReader,
            MockRDFTripleFactory rdfTripleFactory,
            MockRDFTripleStore rdfTripleStore)
        {
            return new InMemoryNodeEventProcessor(
                graphSettingsService,
                graphEventService,
                rdfEventReader,
                rdfTripleFactory,
                rdfTripleStore);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncReturnsImmediatelyWhenQueueIsEmpty()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(0, graphEventService.NodeEvents.Count);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncProcessesNewNode()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(0, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(1, graphEventService.NodeEvents.Count);
            Assert.AreEqual(node, graphEventService.NodeEvents[0].New);
            Assert.IsNull(graphEventService.NodeEvents[0].Old);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncProcessesUpdatedNode()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(1, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);
            var oldNode = new MockNode(subject + "_old");

            var oldPredicate = new HasBlob(GraphName, TypeName, 0).ToString();
            var oldRdfTriple = new RDFTriple(subject, oldPredicate, "oldIndexed", "oldObject", "0", new VersionDetail(0, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);
            rdfTripleFactory.AddNode(oldRdfTriple, oldNode);

            rdfTripleStore.AddResponse(oldPredicate, new GetRDFTriplesResponse(ImmutableList.Create<RDFTriple?>(oldRdfTriple), RDFTripleStoreConsumedCapacity.None()));

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(1, graphEventService.NodeEvents.Count);
            Assert.AreEqual(node, graphEventService.NodeEvents[0].New);
            Assert.AreEqual(oldNode, graphEventService.NodeEvents[0].Old);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncThrowsWhenVersionDetailIsNull()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", null);

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncIgnoresNonHasTypePredicates()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = "SomeOtherPredicate";
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", new VersionDetail(0, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(0, graphEventService.NodeEvents.Count);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncIgnoresHasTypePredicatesForDifferentGraphName()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName("DifferentGraph") + TypeName + "#" + subject;
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", new VersionDetail(0, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(0, graphEventService.NodeEvents.Count);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncProcessesMultipleTriples()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject1 = "node1";
            var subject2 = "node2";
            var predicate1 = HasType.ByGraphName(GraphName) + TypeName + "#" + subject1;
            var predicate2 = HasType.ByGraphName(GraphName) + TypeName + "#" + subject2;
            var versionDetail = new VersionDetail(0, 0);
            var rdfTriple1 = new RDFTriple(subject1, predicate1, "indexedObject1", "object1", "0", versionDetail);
            var rdfTriple2 = new RDFTriple(subject2, predicate2, "indexedObject2", "object2", "0", versionDetail);
            var node1 = new MockNode(subject1);
            var node2 = new MockNode(subject2);

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple1, rdfTriple2));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple1, node1);
            rdfTripleFactory.AddNode(rdfTriple2, node2);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(2, graphEventService.NodeEvents.Count);
            Assert.AreEqual(node1, graphEventService.NodeEvents[0].New);
            Assert.AreEqual(node2, graphEventService.NodeEvents[1].New);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncHandlesCancellation()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var queue = new Queue<ImmutableList<RDFTriple>>();
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(async () =>
            {
                await processor.ProcessInMemoryNodeEventsAsync(cts.Token);
            });
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncRetrievesCorrectOldVersionForUpdate()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(3, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);
            var oldNode = new MockNode(subject + "_old");

            var oldPredicate = new HasBlob(GraphName, TypeName, 2).ToString();
            var oldRdfTriple = new RDFTriple(subject, oldPredicate, "oldIndexed", "oldObject", "0", new VersionDetail(2, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);
            rdfTripleFactory.AddNode(oldRdfTriple, oldNode);

            rdfTripleStore.AddResponse(oldPredicate, new GetRDFTriplesResponse(ImmutableList.Create<RDFTriple?>(oldRdfTriple), RDFTripleStoreConsumedCapacity.None()));

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(1, graphEventService.NodeEvents.Count);
            Assert.AreEqual(node, graphEventService.NodeEvents[0].New);
            Assert.AreEqual(oldNode, graphEventService.NodeEvents[0].Old);
            Assert.AreEqual(1, rdfTripleStore.GetRequests.Count);
            Assert.AreEqual(TableName, rdfTripleStore.GetRequests[0].TableName);
            Assert.AreEqual(subject, rdfTripleStore.GetRequests[0].Keys[0].Subject);
            Assert.AreEqual(oldPredicate, rdfTripleStore.GetRequests[0].Keys[0].Predicate);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncThrowsWhenOldNodeNotFound()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(1, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);

            var oldPredicate = new HasBlob(GraphName, TypeName, 0).ToString();
            rdfTripleStore.AddResponse(oldPredicate, new GetRDFTriplesResponse(ImmutableList.Create((RDFTriple?)null), RDFTripleStoreConsumedCapacity.None()));

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncProcessesMixedNewAndUpdatedNodes()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject1 = "node1";
            var subject2 = "node2";
            var predicate1 = HasType.ByGraphName(GraphName) + TypeName + "#" + subject1;
            var predicate2 = HasType.ByGraphName(GraphName) + TypeName + "#" + subject2;
            var rdfTriple1 = new RDFTriple(subject1, predicate1, "indexedObject1", "object1", "0", new VersionDetail(0, 0));
            var rdfTriple2 = new RDFTriple(subject2, predicate2, "indexedObject2", "object2", "0", new VersionDetail(1, 0));
            var node1 = new MockNode(subject1);
            var node2 = new MockNode(subject2);
            var oldNode2 = new MockNode(subject2 + "_old");

            var oldPredicate2 = new HasBlob(GraphName, TypeName, 0).ToString();
            var oldRdfTriple2 = new RDFTriple(subject2, oldPredicate2, "oldIndexed", "oldObject", "0", new VersionDetail(0, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple1, rdfTriple2));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple1, node1);
            rdfTripleFactory.AddNode(rdfTriple2, node2);
            rdfTripleFactory.AddNode(oldRdfTriple2, oldNode2);

            rdfTripleStore.AddResponse(oldPredicate2, new GetRDFTriplesResponse(ImmutableList.Create<RDFTriple?>(oldRdfTriple2), RDFTripleStoreConsumedCapacity.None()));

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(2, graphEventService.NodeEvents.Count);
            Assert.AreEqual(node1, graphEventService.NodeEvents[0].New);
            Assert.IsNull(graphEventService.NodeEvents[0].Old);
            Assert.AreEqual(node2, graphEventService.NodeEvents[1].New);
            Assert.AreEqual(oldNode2, graphEventService.NodeEvents[1].Old);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncContinuesProcessingUntilQueueEmpty()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(0, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(2, graphEventService.NodeEvents.Count);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncPassesCancellationTokenToNodeEventService()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(0, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            var cts = new CancellationTokenSource();
            await processor.ProcessInMemoryNodeEventsAsync(cts.Token);

            Assert.AreEqual(1, graphEventService.CancellationTokens.Count);
            Assert.AreEqual(cts.Token, graphEventService.CancellationTokens[0]);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncPassesCancellationTokenToTripleStore()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(1, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);
            var oldNode = new MockNode(subject + "_old");

            var oldPredicate = new HasBlob(GraphName, TypeName, 0).ToString();
            var oldRdfTriple = new RDFTriple(subject, oldPredicate, "oldIndexed", "oldObject", "0", new VersionDetail(0, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);
            rdfTripleFactory.AddNode(oldRdfTriple, oldNode);

            rdfTripleStore.AddResponse(oldPredicate, new GetRDFTriplesResponse(ImmutableList.Create<RDFTriple?>(oldRdfTriple), RDFTripleStoreConsumedCapacity.None()));

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            var cts = new CancellationTokenSource();
            await processor.ProcessInMemoryNodeEventsAsync(cts.Token);

            Assert.AreEqual(1, rdfTripleStore.GetCancellationTokens.Count);
            Assert.AreEqual(cts.Token, rdfTripleStore.GetCancellationTokens[0]);
        }

        [TestMethod]
        public async Task ProcessInMemoryNodeEventsAsyncRequestsConsistentReadForOldNode()
        {
            var graphSettings = new MockGraphSettingsService(new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            var graphEventService = new MockGraphEventService();
            var rdfTripleFactory = new MockRDFTripleFactory();
            var rdfTripleStore = new MockRDFTripleStore();

            var subject = "node1";
            var predicate = HasType.ByGraphName(GraphName) + TypeName + "#" + subject;
            var versionDetail = new VersionDetail(1, 0);
            var rdfTriple = new RDFTriple(subject, predicate, "indexedObject", "object", "0", versionDetail);
            var node = new MockNode(subject);
            var oldNode = new MockNode(subject + "_old");

            var oldPredicate = new HasBlob(GraphName, TypeName, 0).ToString();
            var oldRdfTriple = new RDFTriple(subject, oldPredicate, "oldIndexed", "oldObject", "0", new VersionDetail(0, 0));

            var queue = new Queue<ImmutableList<RDFTriple>>();
            queue.Enqueue(ImmutableList.Create(rdfTriple));
            queue.Enqueue(ImmutableList<RDFTriple>.Empty);
            var rdfEventReader = new MockInMemoryRDFEventReader(queue);

            rdfTripleFactory.AddNode(rdfTriple, node);
            rdfTripleFactory.AddNode(oldRdfTriple, oldNode);

            rdfTripleStore.AddResponse(oldPredicate, new GetRDFTriplesResponse(ImmutableList.Create<RDFTriple?>(oldRdfTriple), RDFTripleStoreConsumedCapacity.None()));

            var processor = CreateProcessor(graphSettings, graphEventService, rdfEventReader, rdfTripleFactory, rdfTripleStore);

            await processor.ProcessInMemoryNodeEventsAsync(CancellationToken.None);

            Assert.AreEqual(1, rdfTripleStore.GetRequests.Count);
            Assert.IsTrue(rdfTripleStore.GetRequests[0].ConsistentRead);
        }
    }
}
