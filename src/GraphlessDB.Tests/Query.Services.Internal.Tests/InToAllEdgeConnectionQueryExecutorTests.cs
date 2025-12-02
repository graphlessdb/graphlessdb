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
    public sealed class InToAllEdgeConnectionQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var key = "testKey";

            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            // Verify the GetQueryNodeSourceTypeName lambda
            var extractedTypeName = mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName!(query);
            Assert.AreEqual("TestNodeType", extractedTypeName);
        }

        [TestMethod]
        public void GetQueryNodeSourceTypeNameThrowsForUnexpectedQueryType()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            // Create a different query type to test the exception path
            var unexpectedQuery = new OutToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
                false,
                null);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName!(unexpectedQuery);
            });

            Assert.AreEqual("Unexpected query type", exception.Message);
        }

        [TestMethod]
        public void GetCursorNodeInIdReturnsSubjectFromHasInEdge()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            var cursorNode = new CursorNode(
                null,
                null,
                new HasInEdgeCursor("subject123", "EdgeType", "outId123"),
                null,
                null,
                null,
                null,
                null);

            var extractedId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!(cursorNode);
            Assert.AreEqual("subject123", extractedId);
        }

        [TestMethod]
        public void GetCursorNodeInIdReturnsSubjectFromHasInEdgeProp()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                new HasInEdgePropCursor("subject456", "EdgeType", "outId456", "propValue"),
                null,
                null,
                null,
                null);

            var extractedId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!(cursorNode);
            Assert.AreEqual("subject456", extractedId);
        }

        [TestMethod]
        public void GetCursorNodeInIdThrowsWhenBothAreNull()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!(cursorNode);
            });

            Assert.AreEqual("Node in id was missing", exception.Message);
        }

        [TestMethod]
        public void GetCursorNodeOutIdReturnsNodeOutIdFromHasInEdge()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            var cursorNode = new CursorNode(
                null,
                null,
                new HasInEdgeCursor("subject123", "EdgeType", "outId123"),
                null,
                null,
                null,
                null,
                null);

            var extractedId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!(cursorNode);
            Assert.AreEqual("outId123", extractedId);
        }

        [TestMethod]
        public void GetCursorNodeOutIdReturnsNodeOutIdFromHasInEdgeProp()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                new HasInEdgePropCursor("subject456", "EdgeType", "outId456", "propValue"),
                null,
                null,
                null,
                null);

            var extractedId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!(cursorNode);
            Assert.AreEqual("outId456", extractedId);
        }

        [TestMethod]
        public void GetCursorNodeOutIdThrowsWhenBothAreNull()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
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

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            var cancellationToken = CancellationToken.None;
            var task = executor.ExecuteAsync(context, key, cancellationToken);
            task.Wait();

            var cursorNode = new CursorNode(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!(cursorNode);
            });

            Assert.AreEqual("Node in id was missing", exception.Message);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenEdgesExistAndCursorsDiffer()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
                false,
                null);

            var cursor = "cursor1";
            var endCursor = "endCursor1";
            var childCursor = "differentCursor";
            var node = MockNode.Create("node1");
            var relayEdge = new RelayEdge<INode>(cursor, node);
            var edges = ImmutableList<RelayEdge<INode>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, cursor, endCursor);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);
            var nodeConnectionResult = new NodeConnectionResult(childCursor, cursor, false, false, connection);
            var edgeConnectionResult = new EdgeConnectionResult(childCursor, cursor, false, false, new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, pageInfo));

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
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

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenCursorsMatch()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
                false,
                null);

            var cursor = "cursor1";
            var endCursor = "endCursor1";
            var childCursor = endCursor; // Same as endCursor
            var node = MockNode.Create("node1");
            var relayEdge = new RelayEdge<INode>(cursor, node);
            var edges = ImmutableList<RelayEdge<INode>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, cursor, endCursor);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);
            var nodeConnectionResult = new NodeConnectionResult(childCursor, cursor, false, false, connection);
            var edgeConnectionResult = new EdgeConnectionResult(childCursor, cursor, false, false, new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, pageInfo));

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
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
        public void HasMoreChildDataReturnsFalseWhenEdgesAreEmpty()
        {
            var mockGraphQueryService = new MockGraphQueryService();
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new InToAllEdgeConnectionQueryExecutor(
                mockGraphQueryService,
                mockToEdgeConnectionQueryExecutor);

            var query = new InToAllEdgeConnectionQuery(
                "TestNodeType",
                null,
                null,
                ConnectionArguments.Default,
                100,
                false,
                null);

            var cursor = "cursor1";
            var endCursor = "endCursor1";
            var childCursor = "differentCursor";
            var edges = ImmutableList<RelayEdge<INode>>.Empty; // Empty edges
            var pageInfo = new PageInfo(false, false, cursor, endCursor);
            var connection = new Connection<RelayEdge<INode>, INode>(edges, pageInfo);
            var nodeConnectionResult = new NodeConnectionResult(childCursor, cursor, false, false, connection);
            var edgeConnectionResult = new EdgeConnectionResult(childCursor, cursor, false, false, new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, pageInfo));

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
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

        private sealed class MockGraphQueryService : IGraphQueryService
        {
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
                throw new NotImplementedException();
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
