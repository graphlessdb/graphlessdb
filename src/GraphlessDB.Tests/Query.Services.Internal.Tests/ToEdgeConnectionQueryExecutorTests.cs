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
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class ToEdgeConnectionQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithoutCursor()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var query = CreateToEdgeConnectionQuery();
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode = CreateMockNode("node1");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", childNode)],
                new PageInfo(false, false, "cursor1", "cursor1"));
            var childResult = new NodeConnectionResult(null, "cursor1", false, false, childConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add("childKey", childResult));

            mockGraphQueryService.SetEdgeKeyResponse(new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                    [new RelayEdge<EdgeKey>("edgeCursor1", new EdgeKey("TestEdge", "node1", "node2"))],
                    new PageInfo(false, false, "edgeCursor1", "edgeCursor1"))));

            mockGraphQueryService.SetEdgesResponse(new GetEdgesResponse(
                [new RelayEdge<IEdge>("edgeCursor1", CreateMockEdge("edge1", "node1", "node2"))]));

            mockEdgeFilterService.SetFilteredConnection(new Connection<RelayEdge<IEdge>, IEdge>(
                [new RelayEdge<IEdge>("filteredCursor1", CreateMockEdge("edge1", "node1", "node2"))],
                new PageInfo(false, false, "filteredCursor1", "filteredCursor1")));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                _ => "TestNode",
                _ => "node1",
                _ => "node2",
                (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanRestoreFromCursorWhenNotRootQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var parentCursor = "parentCursor";
            var cursorArgs = new ConnectionArguments(10, parentCursor, null, null);
            var query = new MockToEdgeConnectionQuery("TestEdge", null, null, cursorArgs, 100, false, null);
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode = CreateMockNode("node1");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", childNode)],
                new PageInfo(false, false, "cursor1", "cursor1"));
            var childResult = new NodeConnectionResult(null, "cursor1", false, false, childConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add("childKey", childResult));

            // Setup cursor deserialization to return a valid cursor node
            var cursorNode = CursorNode.Empty with { HasOutEdge = new HasOutEdgeCursor("node1", "TestEdge", "node2") };
            var cursor = Cursor.Create(cursorNode);
            mockCursorSerializer.SetDeserializedCursor(cursor);

            // Setup edge retrieval
            mockGraphQueryService.SetEdge(new RelayEdge<IEdge>("edgeCursor1", CreateMockEdge("edge1", "node1", "node2")));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                _ => "TestNode",
                cn => cn.HasOutEdge?.NodeInId ?? "node1",
                cn => cn.HasOutEdge?.Subject ?? "node2",
                (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task ReturnsEmptyConnectionWhenCursorIsEndOfData()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var parentCursor = "endOfDataCursor";
            var cursorArgs = new ConnectionArguments(10, parentCursor, null, null);
            var query = new MockToEdgeConnectionQuery("TestEdge", null, null, cursorArgs, 100, false, null);
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode = CreateMockNode("node1");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", childNode)],
                new PageInfo(false, false, "cursor1", "cursor1"));
            var childResult = new NodeConnectionResult(null, "cursor1", false, false, childConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add("childKey", childResult));

            // Setup cursor deserialization to return EndOfData cursor
            var cursorNode = CursorNode.CreateEndOfData();
            var cursor = Cursor.Create(cursorNode);
            mockCursorSerializer.SetDeserializedCursor(cursor);

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                _ => "TestNode",
                cn => "node1",
                cn => "node2",
                (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task ThrowsNotSupportedExceptionWhenEdgeTypeNameIsNull()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var parentCursor = "cursor";
            var cursorArgs = new ConnectionArguments(10, parentCursor, null, null);
            var query = new MockToEdgeConnectionQuery(null!, null, null, cursorArgs, 100, false, null);
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode = CreateMockNode("node1");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", childNode)],
                new PageInfo(false, false, "cursor1", "cursor1"));
            var childResult = new NodeConnectionResult(null, "cursor1", false, false, childConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add("childKey", childResult));

            // Setup cursor deserialization to return a valid cursor node (not EndOfData)
            var cursorNode = CursorNode.Empty with { HasOutEdge = new HasOutEdgeCursor("node1", "TestEdge", "node2") };
            var cursor = Cursor.Create(cursorNode);
            mockCursorSerializer.SetDeserializedCursor(cursor);

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await executor.ExecuteAsync(
                    context,
                    key,
                    _ => "TestNode",
                    cn => "node1",
                    cn => "node2",
                    (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                    cancellationToken);
            });
        }

        [TestMethod]
        public async Task ThrowsExceptionWhenDuplicateEdgesDetected()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var query = CreateToEdgeConnectionQuery();
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode = CreateMockNode("node1");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", childNode)],
                new PageInfo(false, false, "cursor1", "cursor1"));
            var childResult = new NodeConnectionResult(null, "cursor1", false, false, childConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add("childKey", childResult));

            mockGraphQueryService.SetEdgeKeyResponse(new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                    [new RelayEdge<EdgeKey>("edgeCursor1", new EdgeKey("TestEdge", "node1", "node2"))],
                    new PageInfo(false, false, "edgeCursor1", "edgeCursor1"))));

            mockGraphQueryService.SetEdgesResponse(new GetEdgesResponse(
                [new RelayEdge<IEdge>("edgeCursor1", CreateMockEdge("edge1", "node1", "node2"))]));

            // Set filtered connection to return duplicates
            mockEdgeFilterService.SetFilteredConnection(new Connection<RelayEdge<IEdge>, IEdge>(
                [
                    new RelayEdge<IEdge>("duplicateCursor", CreateMockEdge("edge1", "node1", "node2")),
                    new RelayEdge<IEdge>("duplicateCursor", CreateMockEdge("edge2", "node1", "node3"))
                ],
                new PageInfo(false, false, "duplicateCursor", "duplicateCursor")));

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await executor.ExecuteAsync(
                    context,
                    key,
                    _ => "TestNode",
                    _ => "node1",
                    _ => "node2",
                    (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                    cancellationToken);
            });
        }

        [TestMethod]
        public async Task CanContinuePaginationWithExistingResult()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var query = CreateToEdgeConnectionQuery();
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode1 = CreateMockNode("node1");
            var childNode2 = CreateMockNode("node2");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [
                    new RelayEdge<INode>("cursor1", childNode1),
                    new RelayEdge<INode>("cursor2", childNode2)
                ],
                new PageInfo(false, false, "cursor1", "cursor2"));
            var childResult = new NodeConnectionResult(null, "cursor2", false, false, childConnection);

            // Create existing result with one edge
            // The ChildCursor needs to match a cursor from the child connection for FromCursorInclusive to work
            var existingEdge = new RelayEdge<IEdge>("existingCursor", CreateMockEdge("edge0", "node0", "node1"));
            var existingConnection = new Connection<RelayEdge<IEdge>, IEdge>(
                [existingEdge],
                new PageInfo(true, false, "existingCursor", "existingCursor"));
            var existingResult = new EdgeConnectionResult("cursor2", "existingCursor", true, true, existingConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add("childKey", childResult)
                    .Add(key, existingResult));

            // Setup cursor operations for continuation
            // The child cursor "cursor2" needs to match the second node's cursor
            var childCursorNode = CursorNode.Empty with { HasType = new HasTypeCursor("node2", "partition", []) };
            var childCursor = Cursor.Create(childCursorNode);

            mockCursorSerializer.AddCursorMapping("cursor2", childCursor);
            mockCursorSerializer.SetDeserializedCursor(childCursor);
            mockCursorSerializer.SetSerializedCursor("cursor2");

            mockGraphQueryService.SetEdgeKeyResponse(new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                    [new RelayEdge<EdgeKey>("edgeCursor2", new EdgeKey("TestEdge", "node2", "node3"))],
                    new PageInfo(false, false, "edgeCursor2", "edgeCursor2"))));

            mockGraphQueryService.SetEdgesResponse(new GetEdgesResponse(
                [new RelayEdge<IEdge>("edgeCursor2", CreateMockEdge("edge2", "node2", "node3"))]));

            mockEdgeFilterService.SetFilteredConnection(new Connection<RelayEdge<IEdge>, IEdge>(
                [new RelayEdge<IEdge>("filteredCursor2", CreateMockEdge("edge2", "node2", "node3"))],
                new PageInfo(false, false, "filteredCursor2", "filteredCursor2")));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                _ => "TestNode",
                _ => "node1",
                _ => "node2",
                (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            // Should have both the existing edge and the new edge
            Assert.AreEqual(2, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task ChecksPostFilteringRequiredForIterationPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockGraphQueryService = new MockGraphQueryService();
            var mockEdgeFilterService = new MockGraphEdgeFilterService();
            mockEdgeFilterService.SetPostFilteringRequired(true);

            var executor = new ToEdgeConnectionQueryExecutor(
                mockCursorSerializer,
                mockGraphQueryService,
                mockEdgeFilterService);

            var key = "testKey";
            var query = new MockToEdgeConnectionQuery("TestEdge", null, null, new ConnectionArguments(10, null, null, null), 50, false, null);
            var childQuery = new NodeConnectionQuery("TestNode", null, null, ConnectionArguments.Default, 10, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode("childKey", new GraphQueryNode(childQuery))
                .AddParentNode("childKey", key, new GraphQueryNode(query));

            var childNode = CreateMockNode("node1");
            var childConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", childNode)],
                new PageInfo(false, false, "cursor1", "cursor1"));
            var childResult = new NodeConnectionResult(null, "cursor1", false, false, childConnection);

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add("childKey", childResult));

            mockGraphQueryService.SetEdgeKeyResponse(new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                    [new RelayEdge<EdgeKey>("edgeCursor1", new EdgeKey("TestEdge", "node1", "node2"))],
                    new PageInfo(false, false, "edgeCursor1", "edgeCursor1"))));

            mockGraphQueryService.SetEdgesResponse(new GetEdgesResponse(
                [new RelayEdge<IEdge>("edgeCursor1", CreateMockEdge("edge1", "node1", "node2"))]));

            mockEdgeFilterService.SetFilteredConnection(new Connection<RelayEdge<IEdge>, IEdge>(
                [new RelayEdge<IEdge>("filteredCursor1", CreateMockEdge("edge1", "node1", "node2"))],
                new PageInfo(false, false, "filteredCursor1", "filteredCursor1")));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                _ => "TestNode",
                _ => "node1",
                _ => "node2",
                (req, ct) => mockGraphQueryService.GetInAndOutToEdgeConnectionAsync(req, ct),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            // Verify that IsPostFilteringRequired was checked
            Assert.IsTrue(mockEdgeFilterService.WasPostFilteringRequiredCalled);
        }

        private static MockToEdgeConnectionQuery CreateToEdgeConnectionQuery()
        {
            return new MockToEdgeConnectionQuery("TestEdge", null, null, ConnectionArguments.Default, 100, false, null);
        }

        private static MockNode CreateMockNode(string id)
        {
            var now = DateTime.UtcNow;
            return new MockNode(id, VersionDetail.New, now, now, DateTime.MinValue);
        }

        private static MockEdge CreateMockEdge(string id, string inId, string outId)
        {
            var now = DateTime.UtcNow;
            return new MockEdge(now, now, DateTime.MinValue, inId, outId);
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
            : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

        private sealed record MockEdge(
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt,
            string InId,
            string OutId)
            : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId);

        private sealed class MockGraphCursorSerializationService : IGraphCursorSerializationService
        {
            private Cursor? _deserializedCursor;
            private string? _serializedCursor;
            private readonly Dictionary<string, Cursor> _cursorMappings = new();

            public void SetDeserializedCursor(Cursor cursor)
            {
                _deserializedCursor = cursor;
            }

            public void SetSerializedCursor(string cursor)
            {
                _serializedCursor = cursor;
            }

            public void AddCursorMapping(string serialized, Cursor deserialized)
            {
                _cursorMappings[serialized] = deserialized;
            }

            public Cursor Deserialize(string cursor)
            {
                if (_cursorMappings.TryGetValue(cursor, out var mappedCursor))
                {
                    return mappedCursor;
                }
                // Return configured cursor or a simple default cursor
                return _deserializedCursor ?? Cursor.Create(CursorNode.Empty);
            }

            public string Serialize(Cursor cursor)
            {
                return _serializedCursor ?? "serializedCursor";
            }
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            private ToEdgeQueryResponse? _edgeKeyResponse;
            private GetEdgesResponse? _edgesResponse;
            private RelayEdge<IEdge>? _edge;

            public void SetEdgeKeyResponse(ToEdgeQueryResponse response)
            {
                _edgeKeyResponse = response;
            }

            public void SetEdgesResponse(GetEdgesResponse response)
            {
                _edgesResponse = response;
            }

            public void SetEdge(RelayEdge<IEdge> edge)
            {
                _edge = edge;
            }

            public Task<RelayEdge<IEdge>> GetEdgeAsync(EdgeKey key, bool consistentRead, CancellationToken cancellationToken)
            {
                return Task.FromResult(_edge ?? throw new InvalidOperationException("No edge set"));
            }

            public Task<GetEdgesResponse> GetEdgesAsync(GetEdgesRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(_edgesResponse ?? throw new InvalidOperationException("No edges response set"));
            }

            public Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(_edgeKeyResponse ?? throw new InvalidOperationException("No edge key response set"));
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
                // Use _edge or _edgesResponse to satisfy requests
                if (_edge != null && request.Keys.Count == 1)
                {
                    return Task.FromResult(new TryGetEdgesResponse(ImmutableList<RelayEdge<IEdge>?>.Empty.Add(_edge)));
                }
                if (_edgesResponse != null)
                {
                    var nullableEdges = _edgesResponse.Edges.Select(e => (RelayEdge<IEdge>?)e).ToImmutableList();
                    return Task.FromResult(new TryGetEdgesResponse(nullableEdges));
                }
                // Return empty response if nothing is set
                return Task.FromResult(new TryGetEdgesResponse(ImmutableList<RelayEdge<IEdge>?>.Empty));
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

            public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockGraphEdgeFilterService : IGraphEdgeFilterService
        {
            private Connection<RelayEdge<IEdge>, IEdge>? _filteredConnection;
            private bool _postFilteringRequired;
            private EdgePushdownQueryData? _pushdownData;

            public bool WasPostFilteringRequiredCalled { get; private set; }

            public void SetFilteredConnection(Connection<RelayEdge<IEdge>, IEdge> connection)
            {
                _filteredConnection = connection;
            }

            public void SetPostFilteringRequired(bool required)
            {
                _postFilteringRequired = required;
            }

            public void SetPushdownData(EdgePushdownQueryData data)
            {
                _pushdownData = data;
            }

            public Task<Connection<RelayEdge<IEdge>, IEdge>> GetFilteredEdgeConnectionAsync(
                Connection<RelayEdge<IEdge>, IEdge> connection,
                IEdgeFilter? filter,
                bool consistentRead,
                CancellationToken cancellationToken)
            {
                return Task.FromResult(_filteredConnection ?? connection);
            }

            public bool IsPostFilteringRequired(IEdgeFilter? filter)
            {
                WasPostFilteringRequiredCalled = true;
                return _postFilteringRequired;
            }

            public EdgePushdownQueryData? TryGetEdgePushdownQueryData(string? edgeTypeName, IEdgeFilter? filter, IEdgeOrder? order)
            {
                return _pushdownData;
            }

            public Task<bool> IsFilterMatchAsync(IEdge edge, IEdgeFilter? filter, bool consistentRead, CancellationToken cancellationToken)
            {
                return Task.FromResult(true);
            }
        }
    }
}
