/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class ToEdgeConnectionQueryExecutorTests
    {
        private static CancellationToken GetCancellationToken()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
        }

        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor, cursor));
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => "inId",
                cn => "outId",
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }


        [TestMethod]
        public async Task ExecuteAsyncWithCursorRestoresEdgeFromCursor()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";
            var edgeTypeName = "TestEdge";
            var nodeId = "node1";
            var inId = "in1";
            var outId = "out1";

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var edge = MockEdge.Create(inId, outId);
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgesToReturn = ImmutableList<RelayEdge<IEdge>?>.Empty.Add(relayEdge);
            var graphQueryService = new MockGraphQueryService(edgesToReturn);
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, new ConnectionArguments(1, cursor, null, null), 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>("nodeCursor", node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, "nodeCursor", "nodeCursor"));
            var nodeResult = new NodeConnectionResult(null, "nodeCursor", false, false, nodeConnection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => inId,
                cn => outId,
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
            Assert.IsFalse(result.NeedsMoreData);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithEndOfDataCursorReturnsEmptyConnection()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";
            var edgeTypeName = "TestEdge";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var cursorNode = CursorNode.CreateEndOfData();
            var cursor = cursorSerializer.Serialize(new Cursor(ImmutableTree<string, CursorNode>.Empty.AddNode("root", cursorNode)));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, new ConnectionArguments(10, cursor, null, null), 100, false, null);
            var node = MockNode.Create("node1");
            var relayNode = new RelayEdge<INode>("nodeCursor", node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, "nodeCursor", "nodeCursor"));
            var nodeResult = new NodeConnectionResult(null, "nodeCursor", false, false, nodeConnection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => "inId",
                cn => "outId",
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithPostFilteringUsesPreFilteredPageSize()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";
            var preFilteredPageSize = 200;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();
            edgeFilterService.SetIsPostFilteringRequired(true);

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, preFilteredPageSize, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor, cursor));
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => "inId",
                cn => "outId",
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ExecuteAsyncThrowsWhenDuplicateEdgesDetected()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";
            var inId = "in1";
            var outId = "out1";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor, cursor));
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);

            var edge = MockEdge.Create(inId, outId);
            var edgeKey = new EdgeKey(edgeTypeName, inId, outId);
            var edgeKeyCursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));
            var edgeKeyEdge = new RelayEdge<EdgeKey>(edgeKeyCursor, edgeKey);
            var edgeKeyConnection = new Connection<RelayEdge<EdgeKey>, EdgeKey>([edgeKeyEdge], new PageInfo(false, false, edgeKeyCursor, edgeKeyCursor));

            var existingEdge = MockEdge.Create(inId, outId);
            var existingRelayEdge = new RelayEdge<IEdge>(cursor, existingEdge);
            var existingEdges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(existingRelayEdge);
            var existingPageInfo = new PageInfo(false, false, cursor, cursor);
            var existingConnection = new Connection<RelayEdge<IEdge>, IEdge>(existingEdges, existingPageInfo);
            var existingResult = new EdgeConnectionResult(null, cursor, true, false, existingConnection);

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult)
                    .Add(key, existingResult));

            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgesToReturn = ImmutableList<RelayEdge<IEdge>?>.Empty.Add(relayEdge);
            var graphQueryServiceWithEdges = new MockGraphQueryService(edgesToReturn);
            var executorWithEdges = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryServiceWithEdges,
                edgeFilterService);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await executorWithEdges.ExecuteAsync(
                    context,
                    key,
                    q => "TestNodeType",
                    cn => inId,
                    cn => outId,
                    (req, ct) => Task.FromResult(new ToEdgeQueryResponse(edgeKeyConnection)),
                    cancellationToken);
            });
        }

        [TestMethod]
        public async Task ExecuteAsyncThrowsWhenEdgeTypeNameIsNull()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";
            var nodeId = "node1";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(null, null, null, new ConnectionArguments(10, cursor, null, null), 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>("nodeCursor", node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, "nodeCursor", "nodeCursor"));
            var nodeResult = new NodeConnectionResult(null, "nodeCursor", false, false, nodeConnection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await executor.ExecuteAsync(
                    context,
                    key,
                    q => "TestNodeType",
                    cn => "inId",
                    cn => "outId",
                    (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                    cancellationToken);
            });
        }

        [TestMethod]
        public async Task ExecuteAsyncWithExistingCursorThrowsWhenChildCursorIsNull()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var cursorNode = CursorNode.CreateEndOfData();
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>("nodeCursor", node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, "nodeCursor", "nodeCursor"));
            var nodeResult = new NodeConnectionResult(null, "nodeCursor", false, false, nodeConnection);

            var existingResult = new EdgeConnectionResult(null, cursor, true, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult)
                    .Add(key, existingResult));

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await executor.ExecuteAsync(
                    context,
                    key,
                    q => "TestNodeType",
                    cn => "inId",
                    cn => "outId",
                    (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                    cancellationToken);
            });
        }

        [TestMethod]
        public async Task ExecuteAsyncWithExistingChildCursorThrowsWhenHasNextPage()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var childCursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(childCursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, childCursor, childCursor));
            var nodeResult = new NodeConnectionResult(null, childCursor, false, false, nodeConnection);

            var existingPageInfo = new PageInfo(true, false, "", "");
            var existingResult = new EdgeConnectionResult(childCursor, "", true, false, new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, existingPageInfo));

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult)
                    .Add(key, existingResult));

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await executor.ExecuteAsync(
                    context,
                    key,
                    q => "TestNodeType",
                    cn => "inId",
                    cn => "outId",
                    (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                    cancellationToken);
            });
        }

        [TestMethod]
        public async Task ExecuteAsyncWithMultipleNodesUsesOutIdForChildCursor()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId1 = "node1";
            var nodeId2 = "node2";
            var edgeTypeName = "TestEdge";
            var inId = "in1";
            var outId = nodeId2;

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor1 = new HasTypeCursor(nodeId1, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode1 = new CursorNode(hasTypeCursor1, null, null, null, null, null, null, null);
            var cursor1 = cursorSerializer.Serialize(Cursor.Create(cursorNode1));

            var hasTypeCursor2 = new HasTypeCursor(nodeId2, "partition2", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode2 = new CursorNode(hasTypeCursor2, null, null, null, null, null, null, null);
            var cursor2 = cursorSerializer.Serialize(Cursor.Create(cursorNode2));

            var edge = MockEdge.Create(inId, outId);
            var relayEdge = new RelayEdge<IEdge>(cursor1, edge);
            var edgesToReturn = ImmutableList<RelayEdge<IEdge>?>.Empty.Add(relayEdge);
            var graphQueryService = new MockGraphQueryService(edgesToReturn);
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, 100, false, null);
            var node1 = MockNode.Create(nodeId1);
            var node2 = MockNode.Create(nodeId2);
            var relayNode1 = new RelayEdge<INode>(cursor1, node1);
            var relayNode2 = new RelayEdge<INode>(cursor2, node2);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode1, relayNode2], new PageInfo(false, false, cursor1, cursor2));
            var nodeResult = new NodeConnectionResult(null, cursor1, false, false, nodeConnection);

            var edgeKey = new EdgeKey(edgeTypeName, inId, outId);
            var edgeKeyCursor = cursorSerializer.Serialize(Cursor.Create(cursorNode1));
            var edgeKeyEdge = new RelayEdge<EdgeKey>(edgeKeyCursor, edgeKey);
            var edgeKeyConnection = new Connection<RelayEdge<EdgeKey>, EdgeKey>([edgeKeyEdge], new PageInfo(false, false, edgeKeyCursor, edgeKeyCursor));

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => inId,
                cn => outId,
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(edgeKeyConnection)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        private sealed record MockToEdgeConnectionQuery(
            string? EdgeTypeName,
            IEdgeFilter? Filter,
            IEdgeOrder? Order,
            ConnectionArguments Page,
            int PreFilteredPageSize,
            bool ConsistentRead,
            string? Tag)
            : ToEdgeConnectionQuery(EdgeTypeName, Filter, Order, Page, PreFilteredPageSize, ConsistentRead, Tag);

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
            string OutId)
            : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
        {
            public static MockEdge Create(string inId, string outId)
            {
                var now = DateTime.UtcNow;
                return new MockEdge(now, now, DateTime.MinValue, inId, outId);
            }
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            private readonly ImmutableList<RelayEdge<IEdge>?> edgesToReturn;

            public MockGraphQueryService(ImmutableList<RelayEdge<IEdge>?>? edges = null)
            {
                edgesToReturn = edges ?? ImmutableList<RelayEdge<IEdge>?>.Empty;
            }

            public Task ClearAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(TryGetVersionedNodesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetEdgesResponse> TryGetEdgesAsync(TryGetEdgesRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(new TryGetEdgesResponse(edgesToReturn));
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAsync(GetConnectionByTypeRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(GetConnectionByTypeAndPropertyNameRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(GetConnectionByTypePropertyNameAndValueRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(GetConnectionByTypePropertyNameAndValuesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetInToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockEdgeFilterService : IGraphEdgeFilterService
        {
            private bool isPostFilteringRequired;
            private Connection<RelayEdge<IEdge>, IEdge>? filteredConnection;

            public void SetIsPostFilteringRequired(bool value)
            {
                isPostFilteringRequired = value;
            }

            public void SetFilteredConnection(Connection<RelayEdge<IEdge>, IEdge> connection)
            {
                filteredConnection = connection;
            }

            public bool IsPostFilteringRequired(IEdgeFilter? filter)
            {
                return isPostFilteringRequired;
            }

            public EdgePushdownQueryData? TryGetEdgePushdownQueryData(string? edgeTypeName, IEdgeFilter? filter, IEdgeOrder? order)
            {
                return null;
            }

            public Task<bool> IsFilterMatchAsync(IEdge edge, IEdgeFilter? filter, bool consistentRead, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<Connection<RelayEdge<IEdge>, IEdge>> GetFilteredEdgeConnectionAsync(
                Connection<RelayEdge<IEdge>, IEdge> connection,
                IEdgeFilter? filter,
                bool consistentRead,
                CancellationToken cancellationToken)
            {
                return Task.FromResult(filteredConnection ?? connection);
            }
        }
    }
}
