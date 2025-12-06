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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class FromEdgeConnectionQueryExecutorTests
    {
        private static CancellationToken GetCancellationToken()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithInFromEdgeConnectionQuery()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node1");
            mockQueryService.AddNode("node1", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.AreEqual(1, result.Connection.Edges.Count);
            Assert.AreEqual("node1", result.Connection.Edges[0].Node.Id);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithOutFromEdgeConnectionQuery()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new OutFromEdgeConnectionQuery(null, false, null);
            var childQuery = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node2");
            mockQueryService.AddNode("node2", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.OutId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
            Assert.AreEqual("node2", result.Connection.Edges[0].Node.Id);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithMultipleEdges()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge1 = MockEdge.Create("edge1", "node1", "node3");
            var edge2 = MockEdge.Create("edge2", "node2", "node3");
            var relayEdge1 = new RelayEdge<IEdge>("cursor1", edge1);
            var relayEdge2 = new RelayEdge<IEdge>("cursor2", edge2);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge1, relayEdge2),
                new PageInfo(false, false, "cursor1", "cursor2"));
            var edgeResult = new EdgeConnectionResult(null, "cursor2", false, false, edgeConnection);

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");
            mockQueryService.AddNode("node1", node1);
            mockQueryService.AddNode("node2", node2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithDuplicateTargetNodes()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge1 = MockEdge.Create("edge1", "node1", "node3");
            var edge2 = MockEdge.Create("edge2", "node1", "node3");
            var relayEdge1 = new RelayEdge<IEdge>("cursor1", edge1);
            var relayEdge2 = new RelayEdge<IEdge>("cursor2", edge2);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge1, relayEdge2),
                new PageInfo(false, false, "cursor1", "cursor2"));
            var edgeResult = new EdgeConnectionResult(null, "cursor2", false, false, edgeConnection);

            var node1 = MockNode.Create("node1");
            mockQueryService.AddNode("node1", node1);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
            Assert.AreEqual("node1", result.Connection.Edges[0].Node.Id);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithEmptyEdgeConnection()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edgeConnection = Connection<RelayEdge<IEdge>, IEdge>.Empty;
            var edgeResult = new EdgeConnectionResult(null, "endcursor", false, false, edgeConnection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithConsistentRead()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, true, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node1");
            mockQueryService.AddNode("node1", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsTrue(mockQueryService.LastConsistentRead);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithExistingResult()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node2", "node3");
            var relayEdge = new RelayEdge<IEdge>("cursor2", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor2", "cursor2"));
            var edgeResult = new EdgeConnectionResult(null, "cursor2", false, false, edgeConnection);

            var existingNode = MockNode.Create("node1");
            var existingRelayNode = new RelayEdge<INode>("cursor1", existingNode);
            var existingConnection = new Connection<RelayEdge<INode>, INode>(
                ImmutableList.Create(existingRelayNode),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var existingResult = new NodeConnectionResult(null, "cursor1", false, false, existingConnection);

            var node2 = MockNode.Create("node2");
            mockQueryService.AddNode("node2", node2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, edgeResult)
                    .Add(key, existingResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNodeFilter()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var filter = new MockNodeFilter();
            var query = new InFromEdgeConnectionQuery(filter, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node1");
            mockQueryService.AddNode("node1", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsTrue(mockFilterService.WasFilterCalled);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithMultipleTargetIdsPerEdge()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InAndOutFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InAndOutToEdgeConnectionQuery("EdgeType", "NodeType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");
            mockQueryService.AddNode("node1", node1);
            mockQueryService.AddNode("node2", node2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId, e.OutId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithDanglingNode()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "missingNode", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            // Don't add missingNode to the mock service

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithInToAllEdgeConnectionQuery()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new InToAllEdgeConnectionQuery("NodeInType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node1");
            mockQueryService.AddNode("node1", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithOutToAllEdgeConnectionQuery()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new OutFromEdgeConnectionQuery(null, false, null);
            var childQuery = new OutToAllEdgeConnectionQuery("NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node2");
            mockQueryService.AddNode("node2", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.OutId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithWhereEdgeConnectionQuery()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var mockQueryService = new MockGraphQueryService();
            var mockFilterService = new MockGraphNodeFilterService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FromEdgeConnectionQueryExecutor(
                mockQueryService,
                mockFilterService,
                mockCursorSerializer);

            var query = new InFromEdgeConnectionQuery(null, false, null);
            var childQuery = new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null);

            var edge = MockEdge.Create("edge1", "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edgeConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                ImmutableList.Create(relayEdge),
                new PageInfo(false, false, "cursor1", "cursor1"));
            var edgeResult = new EdgeConnectionResult(null, "cursor1", false, false, edgeConnection);

            var node = MockNode.Create("node1");
            mockQueryService.AddNode("node1", node);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(childQuery))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                e => ImmutableList.Create(e.InId),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            private readonly Dictionary<string, INode> nodes = new();

            public bool LastConsistentRead { get; private set; }

            public void AddNode(string id, INode node)
            {
                nodes[id] = node;
            }

            public Task ClearAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(
                GetConnectionByTypeAndPropertyNameRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAsync(
                GetConnectionByTypeRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(
                GetConnectionByTypePropertyNameAndValueRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(
                GetConnectionByTypePropertyNameAndValuesRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(
                ToEdgeQueryRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetInToEdgeConnectionAsync(
                ToEdgeQueryRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(
                ToEdgeQueryRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetEdgesResponse> TryGetEdgesAsync(
                TryGetEdgesRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetNodesResponse> TryGetNodesAsync(
                TryGetNodesRequest request,
                CancellationToken cancellationToken)
            {
                LastConsistentRead = request.ConsistentRead;

                var resultNodes = request.Ids
                    .Select(id => nodes.TryGetValue(id, out var node)
                        ? new RelayEdge<INode>("cursor_" + id, node)
                        : null)
                    .ToImmutableList();

                return Task.FromResult(new TryGetNodesResponse(resultNodes));
            }

            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(
                TryGetVersionedNodesRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockGraphNodeFilterService : IGraphNodeFilterService
        {
            public bool WasFilterCalled { get; private set; }

            public bool IsPostFilteringRequired(INodeFilter? filter)
            {
                return filter != null;
            }

            public Task<bool> IsFilterMatchAsync(INode node, INodeFilter? filter, bool consistentRead, CancellationToken cancellationToken)
            {
                WasFilterCalled = true;
                return Task.FromResult(true);
            }

            public NodePushdownQueryData? TryGetNodePushdownQueryData(string type, INodeFilter? filter, INodeOrder? order, CancellationToken cancellationToken)
            {
                return null;
            }
        }

        private sealed class MockGraphCursorSerializationService : IGraphCursorSerializationService
        {
            public Cursor Deserialize(string cursor)
            {
                return Cursor.Create(CursorNode.CreateEndOfData());
            }

            public string Serialize(Cursor cursor)
            {
                return "serialized_cursor";
            }
        }

        private sealed class MockNodeFilter : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
        }

        private sealed record MockNode(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt)
            : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
        {
            public static MockNode Create(string id)
            {
                var now = DateTime.UtcNow;
                return new MockNode(id, VersionDetail.New, now, now, DateTime.MinValue);
            }
        }

        private sealed record MockEdge(
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt,
            string InId,
            string OutId,
            string Id)
            : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
        {
            public static MockEdge Create(string id, string inId, string outId)
            {
                var now = DateTime.UtcNow;
                return new MockEdge(now, now, DateTime.MinValue, inId, outId, id);
            }
        }
    }
}
