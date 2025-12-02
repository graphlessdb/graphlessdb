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
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class OutToAllEdgeConnectionQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.AreEqual(context, mockToEdgeConnectionQueryExecutor.LastContext);
            Assert.AreEqual(key, mockToEdgeConnectionQueryExecutor.LastKey);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetToEdgeConnectionAsync);
            Assert.AreEqual(cancellationToken, mockToEdgeConnectionQueryExecutor.LastCancellationToken);
        }

        [TestMethod]
        public void GetQueryNodeSourceTypeNameReturnsNodeOutTypeNameForOutToAllEdgeConnectionQuery()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getQueryNodeSourceTypeName = mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName!;
            var typeName = getQueryNodeSourceTypeName(query);

            Assert.AreEqual("TestNodeOut", typeName);
        }

        [TestMethod]
        public void GetQueryNodeSourceTypeNameThrowsForUnexpectedQueryType()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getQueryNodeSourceTypeName = mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName!;

            // Create a different query type
            var unexpectedQuery = new InToAllEdgeConnectionQuery(
                "TestNodeIn",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            Assert.ThrowsException<GraphlessDBOperationException>(() => getQueryNodeSourceTypeName(unexpectedQuery));
        }

        [TestMethod]
        public void GetCursorNodeInIdReturnsSubjectFromHasInEdge()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getCursorNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!;

            var cursorNode = new CursorNode(
                null,
                null,
                new HasInEdgeCursor("testSubject", "EdgeType", "testNodeOut"),
                null,
                null,
                null,
                null,
                null);

            var inId = getCursorNodeInId(cursorNode);

            Assert.AreEqual("testSubject", inId);
        }

        [TestMethod]
        public void GetCursorNodeInIdReturnsSubjectFromHasInEdgeProp()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getCursorNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!;

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                new HasInEdgePropCursor("testSubject", "EdgeType", "testNodeOut", "propValue"),
                null,
                null,
                null,
                null);

            var inId = getCursorNodeInId(cursorNode);

            Assert.AreEqual("testSubject", inId);
        }

        [TestMethod]
        public void GetCursorNodeInIdThrowsWhenBothAreNull()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getCursorNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!;

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);

            Assert.ThrowsException<GraphlessDBOperationException>(() => getCursorNodeInId(cursorNode));
        }

        [TestMethod]
        public void GetCursorNodeOutIdReturnsNodeOutIdFromHasInEdge()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getCursorNodeOutId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!;

            var cursorNode = new CursorNode(
                null,
                null,
                new HasInEdgeCursor("testSubject", "EdgeType", "testNodeOut"),
                null,
                null,
                null,
                null,
                null);

            var outId = getCursorNodeOutId(cursorNode);

            Assert.AreEqual("testNodeOut", outId);
        }

        [TestMethod]
        public void GetCursorNodeOutIdReturnsNodeOutIdFromHasInEdgeProp()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getCursorNodeOutId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!;

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                new HasInEdgePropCursor("testSubject", "EdgeType", "testNodeOut", "propValue"),
                null,
                null,
                null,
                null);

            var outId = getCursorNodeOutId(cursorNode);

            Assert.AreEqual("testNodeOut", outId);
        }

        [TestMethod]
        public void GetCursorNodeOutIdThrowsWhenBothAreNull()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var _ = executor.ExecuteAsync(context, key, cancellationToken).GetAwaiter().GetResult();

            var getCursorNodeOutId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!;

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);

            Assert.ThrowsException<GraphlessDBOperationException>(() => getCursorNodeOutId(cursorNode));
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenEdgesAreEmpty()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var childKey = "childKey";
            var cursor = "cursor1";

            var edges = ImmutableList<RelayEdge<INode>>.Empty;
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);
            var nodeConnectionResult = new NodeConnectionResult(null, cursor, false, false, connection);

            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new NodeConnectionQuery("TestType", null, null, ConnectionArguments.Default, 25, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeConnectionResult)
                    .Add(key, edgeConnectionResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenChildCursorEqualsEndCursor()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var childKey = "childKey";
            var cursor = "cursor1";
            var nodeId = "node1";

            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var edges = ImmutableList<RelayEdge<INode>>.Empty.Add(relayNode);
            var pageInfo = new PageInfo(false, false, cursor, cursor);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);
            var nodeConnectionResult = new NodeConnectionResult(null, cursor, false, false, connection);

            var edgeConnectionResult = new EdgeConnectionResult(cursor, cursor, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new NodeConnectionQuery("TestType", null, null, ConnectionArguments.Default, 25, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeConnectionResult)
                    .Add(key, edgeConnectionResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenChildCursorNotEqualsEndCursor()
        {
            var key = "testKey";
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new OutToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new OutToAllEdgeConnectionQuery(
                "TestNodeOut",
                null,
                null,
                ConnectionArguments.Default,
                25,
                false,
                null);

            var childKey = "childKey";
            var cursor1 = "cursor1";
            var cursor2 = "cursor2";
            var nodeId = "node1";

            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor1, node);
            var edges = ImmutableList<RelayEdge<INode>>.Empty.Add(relayNode);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);
            var nodeConnectionResult = new NodeConnectionResult(null, cursor1, false, false, connection);

            var edgeConnectionResult = new EdgeConnectionResult(cursor1, cursor1, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new NodeConnectionQuery("TestType", null, null, ConnectionArguments.Default, 25, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeConnectionResult)
                    .Add(key, edgeConnectionResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsTrue(result);
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            public Task ClearAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(GetConnectionByTypeAndPropertyNameRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<GetConnectionResponse> GetConnectionByTypeAsync(GetConnectionByTypeRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(GetConnectionByTypePropertyNameAndValueRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(GetConnectionByTypePropertyNameAndValuesRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<ToEdgeQueryResponse> GetInToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task PutAsync(PutRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<TryGetEdgesResponse> TryGetEdgesAsync(TryGetEdgesRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(TryGetVersionedNodesRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
        }

        private sealed class MockToEdgeConnectionQueryExecutor : IToEdgeConnectionQueryExecutor
        {
            private GraphExecutionContext? _result;
            public GraphExecutionContext? LastContext { get; private set; }
            public string? LastKey { get; private set; }
            public Func<ToEdgeConnectionQuery, string>? LastGetQueryNodeSourceTypeName { get; private set; }
            public Func<CursorNode, string>? LastGetCursorNodeInId { get; private set; }
            public Func<CursorNode, string>? LastGetCursorNodeOutId { get; private set; }
            public Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>>? LastGetToEdgeConnectionAsync { get; private set; }
            public CancellationToken LastCancellationToken { get; private set; }

            public void SetResult(GraphExecutionContext result)
            {
                _result = result;
            }

            public Task<GraphExecutionContext> ExecuteAsync(
                GraphExecutionContext context,
                string key,
                Func<ToEdgeConnectionQuery, string> getQueryNodeSourceTypeName,
                Func<CursorNode, string> getCursorNodeInId,
                Func<CursorNode, string> getCursorNodeOutId,
                Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>> getToEdgeConnectionAsync,
                CancellationToken cancellationToken)
            {
                LastContext = context;
                LastKey = key;
                LastGetQueryNodeSourceTypeName = getQueryNodeSourceTypeName;
                LastGetCursorNodeInId = getCursorNodeInId;
                LastGetCursorNodeOutId = getCursorNodeOutId;
                LastGetToEdgeConnectionAsync = getToEdgeConnectionAsync;
                LastCancellationToken = cancellationToken;
                return Task.FromResult(_result ?? context);
            }
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
    }
}
