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
    public sealed class InAndOutToEdgeConnectionQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.IsTrue(mockToEdgeConnectionQueryExecutor.ExecuteAsyncWasCalled);
            Assert.AreEqual(context, mockToEdgeConnectionQueryExecutor.LastContext);
            Assert.AreEqual(key, mockToEdgeConnectionQueryExecutor.LastKey);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetToEdgeConnectionAsync);
        }

        [TestMethod]
        public void CanGetQueryNodeSourceTypeNameFromInAndOutQuery()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getQueryNodeSourceTypeName = mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName;
            Assert.IsNotNull(getQueryNodeSourceTypeName);

            var result = getQueryNodeSourceTypeName(query);
            Assert.AreEqual("TestNode", result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ThrowsExceptionWhenQueryTypeIsUnexpected()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getQueryNodeSourceTypeName = mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName;
            Assert.IsNotNull(getQueryNodeSourceTypeName);

            // Create an unexpected query type to test exception
            var unexpectedQuery = new OutToEdgeConnectionQuery(
                "TestEdge",
                "InTypeName",
                "OutTypeName",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            getQueryNodeSourceTypeName(unexpectedQuery);
        }

        [TestMethod]
        public void CanGetCursorNodeInIdFromHasInEdge()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getCursorNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId;
            Assert.IsNotNull(getCursorNodeInId);

            var cursorNode = new CursorNode(
                null,
                null,
                new HasInEdgeCursor("nodeIn123", "EdgeType", "nodeOut456"),
                null,
                null,
                null,
                null,
                null);

            var result = getCursorNodeInId(cursorNode);
            Assert.AreEqual("nodeIn123", result);
        }

        [TestMethod]
        public void CanGetCursorNodeInIdFromHasInEdgeProp()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getCursorNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId;
            Assert.IsNotNull(getCursorNodeInId);

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                new HasInEdgePropCursor("nodeIn789", "EdgeType", "nodeOut012", "propValue"),
                null,
                null,
                null,
                null);

            var result = getCursorNodeInId(cursorNode);
            Assert.AreEqual("nodeIn789", result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ThrowsExceptionWhenCursorNodeInIdIsMissing()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getCursorNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId;
            Assert.IsNotNull(getCursorNodeInId);

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);

            getCursorNodeInId(cursorNode);
        }

        [TestMethod]
        public void CanGetCursorNodeOutIdFromHasInEdge()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getCursorNodeOutId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId;
            Assert.IsNotNull(getCursorNodeOutId);

            var cursorNode = new CursorNode(
                null,
                null,
                new HasInEdgeCursor("nodeIn123", "EdgeType", "nodeOut456"),
                null,
                null,
                null,
                null,
                null);

            var result = getCursorNodeOutId(cursorNode);
            Assert.AreEqual("nodeOut456", result);
        }

        [TestMethod]
        public void CanGetCursorNodeOutIdFromHasInEdgeProp()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getCursorNodeOutId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId;
            Assert.IsNotNull(getCursorNodeOutId);

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                new HasInEdgePropCursor("nodeIn789", "EdgeType", "nodeOut012", "propValue"),
                null,
                null,
                null,
                null);

            var result = getCursorNodeOutId(cursorNode);
            Assert.AreEqual("nodeOut012", result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ThrowsExceptionWhenCursorNodeOutIdIsMissing()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            executor.ExecuteAsync(context, key, cancellationToken).Wait(cancellationToken);

            var getCursorNodeOutId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId;
            Assert.IsNotNull(getCursorNodeOutId);

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);

            getCursorNodeOutId(cursorNode);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenMoreDataAvailable()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var childKey = "childKey";

            var edge = CreateMockEdge("edge1", "in1", "out1");
            var edges = ImmutableList<RelayEdge<INode>>.Empty.Add(
                new RelayEdge<INode>("cursor1", CreateMockNode("node1")));

            var pageInfo = new PageInfo(false, true, "startCursor", "endCursor");
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);

            var childResult = new NodeConnectionResult(
                null,
                "cursor1",
                false,
                false,
                connection);

            var edgeConnectionResult = new EdgeConnectionResult(
                "differentCursor",
                "cursor2",
                false,
                false,
                new Connection<RelayEdge<IEdge>, IEdge>(
                    ImmutableList<RelayEdge<IEdge>>.Empty,
                    new PageInfo(false, false, string.Empty, string.Empty)));

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new NodeConnectionQuery("Node", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, edgeConnectionResult)
                    .Add(childKey, childResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenNoMoreData()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var childKey = "childKey";

            var edges = ImmutableList<RelayEdge<INode>>.Empty.Add(
                new RelayEdge<INode>("cursor1", CreateMockNode("node1")));

            var pageInfo = new PageInfo(false, true, "startCursor", "endCursor");
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);

            var childResult = new NodeConnectionResult(
                null,
                "cursor1",
                false,
                false,
                connection);

            var edgeConnectionResult = new EdgeConnectionResult(
                "endCursor",
                "cursor2",
                false,
                false,
                new Connection<RelayEdge<IEdge>, IEdge>(
                    ImmutableList<RelayEdge<IEdge>>.Empty,
                    new PageInfo(false, false, string.Empty, string.Empty)));

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new NodeConnectionQuery("Node", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, edgeConnectionResult)
                    .Add(childKey, childResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenEdgesAreEmpty()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();

            var executor = new InAndOutToEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var childKey = "childKey";

            var edges = ImmutableList<RelayEdge<INode>>.Empty;

            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);

            var childResult = new NodeConnectionResult(
                null,
                "cursor1",
                false,
                false,
                connection);

            var edgeConnectionResult = new EdgeConnectionResult(
                "differentCursor",
                "cursor2",
                false,
                false,
                new Connection<RelayEdge<IEdge>, IEdge>(
                    ImmutableList<RelayEdge<IEdge>>.Empty,
                    new PageInfo(false, false, string.Empty, string.Empty)));

            var query = new InAndOutToEdgeConnectionQuery(
                "TestEdge",
                "TestNode",
                null,
                null,
                ConnectionArguments.Default,
                10,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new NodeConnectionQuery("Node", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, edgeConnectionResult)
                    .Add(childKey, childResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        private static MockNode CreateMockNode(string id)
        {
            var now = DateTime.UtcNow;
            return new MockNode(id, VersionDetail.New, now, now, DateTime.MinValue);
        }

        private static MockEdge CreateMockEdge(string id, string inId, string outId)
        {
            var now = DateTime.UtcNow;
            return new MockEdge(now, now, DateTime.MinValue, inId, outId, id);
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
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
                return Task.FromResult(new ToEdgeQueryResponse(
                    Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty));
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
                throw new NotImplementedException();
            }

            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(
                TryGetVersionedNodesRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockToEdgeConnectionQueryExecutor : IToEdgeConnectionQueryExecutor
        {
            public bool ExecuteAsyncWasCalled { get; private set; }
            public GraphExecutionContext? LastContext { get; private set; }
            public string? LastKey { get; private set; }
            public Func<ToEdgeConnectionQuery, string>? LastGetQueryNodeSourceTypeName { get; private set; }
            public Func<CursorNode, string>? LastGetCursorNodeInId { get; private set; }
            public Func<CursorNode, string>? LastGetCursorNodeOutId { get; private set; }
            public Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>>? LastGetToEdgeConnectionAsync { get; private set; }

            public Task<GraphExecutionContext> ExecuteAsync(
                GraphExecutionContext context,
                string key,
                Func<ToEdgeConnectionQuery, string> getQueryNodeSourceTypeName,
                Func<CursorNode, string> getCursorNodeInId,
                Func<CursorNode, string> getCursorNodeOutId,
                Func<ToEdgeQueryRequest, CancellationToken, Task<ToEdgeQueryResponse>> getToEdgeConnectionAsync,
                CancellationToken cancellationToken)
            {
                ExecuteAsyncWasCalled = true;
                LastContext = context;
                LastKey = key;
                LastGetQueryNodeSourceTypeName = getQueryNodeSourceTypeName;
                LastGetCursorNodeInId = getCursorNodeInId;
                LastGetCursorNodeOutId = getCursorNodeOutId;
                LastGetToEdgeConnectionAsync = getToEdgeConnectionAsync;

                return Task.FromResult(context);
            }
        }

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
            string OutId,
            string Id)
            : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId);
    }
}
