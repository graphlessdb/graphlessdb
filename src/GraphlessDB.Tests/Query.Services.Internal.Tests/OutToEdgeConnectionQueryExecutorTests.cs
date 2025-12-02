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
    public sealed class OutToEdgeConnectionQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var key = "testKey";
            var cursor = "cursor1";
            var nodeId = "node1";

            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var edgeConnectionResult = new EdgeConnectionResult(cursor, cursor, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context.SetResult(key, edgeConnectionResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.AreEqual(context, mockToEdgeConnectionQueryExecutor.LastContext);
            Assert.AreEqual(key, mockToEdgeConnectionQueryExecutor.LastKey);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId);
            Assert.IsNotNull(mockToEdgeConnectionQueryExecutor.LastGetToEdgeConnectionAsync);
            Assert.AreEqual(cancellationToken, mockToEdgeConnectionQueryExecutor.LastCancellationToken);

            // Verify GetQueryNodeSourceTypeName extracts NodeOutTypeName
            var extractedTypeName = mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName!(query);
            Assert.AreEqual("NodeOutType", extractedTypeName);
        }

        [TestMethod]
        public void GetQueryNodeSourceTypeNameThrowsOnUnexpectedQueryType()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Now test with an unexpected query type - create a mock ToEdgeConnectionQuery that is not OutToEdgeConnectionQuery
            var unexpectedQuery = new MockToEdgeConnectionQuery("EdgeType", null, null, ConnectionArguments.Default, 100, false, null);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                mockToEdgeConnectionQueryExecutor.LastGetQueryNodeSourceTypeName!(unexpectedQuery);
            });

            Assert.AreEqual("Unexpected query type", exception.Message);
        }

        [TestMethod]
        public void GetCursorNodeInIdReturnsNodeInIdFromHasOutEdge()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Test with HasOutEdge
            var cursorNode = new CursorNode(null, null, null, null, new HasOutEdgeCursor("subject1", "EdgeType", "nodeInId1"), null, null, null);
            var extractedNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!(cursorNode);
            Assert.AreEqual("nodeInId1", extractedNodeInId);
        }

        [TestMethod]
        public void GetCursorNodeInIdReturnsNodeInIdFromHasOutEdgeProp()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Test with HasOutEdgeProp
            var cursorNode = new CursorNode(null, null, null, null, null, new HasOutEdgePropCursor("subject1", "EdgeType", "nodeInId1", "propValue"), null, null);
            var extractedNodeInId = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!(cursorNode);
            Assert.AreEqual("nodeInId1", extractedNodeInId);
        }

        [TestMethod]
        public void GetCursorNodeInIdThrowsWhenNodeInIdIsMissing()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Test with a CursorNode that has neither HasOutEdge nor HasOutEdgeProp
            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                mockToEdgeConnectionQueryExecutor.LastGetCursorNodeInId!(cursorNode);
            });

            Assert.AreEqual("Node in id was missing", exception.Message);
        }

        [TestMethod]
        public void GetCursorNodeOutIdReturnsSubjectFromHasOutEdge()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Test with HasOutEdge
            var cursorNode = new CursorNode(null, null, null, null, new HasOutEdgeCursor("subject1", "EdgeType", "nodeInId1"), null, null, null);
            var extractedSubject = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!(cursorNode);
            Assert.AreEqual("subject1", extractedSubject);
        }

        [TestMethod]
        public void GetCursorNodeOutIdReturnsSubjectFromHasOutEdgeProp()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Test with HasOutEdgeProp
            var cursorNode = new CursorNode(null, null, null, null, null, new HasOutEdgePropCursor("subject1", "EdgeType", "nodeInId1", "propValue"), null, null);
            var extractedSubject = mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!(cursorNode);
            Assert.AreEqual("subject1", extractedSubject);
        }

        [TestMethod]
        public void GetCursorNodeOutIdThrowsWhenSubjectIsMissing()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var key = "testKey";
            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockToEdgeConnectionQueryExecutor.SetResult(context);

            // Execute to capture the lambda
            var cancellationToken = CancellationToken.None;
            _ = executor.ExecuteAsync(context, key, cancellationToken).Result;

            // Test with a CursorNode that has neither HasOutEdge nor HasOutEdgeProp
            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                mockToEdgeConnectionQueryExecutor.LastGetCursorNodeOutId!(cursorNode);
            });

            Assert.AreEqual("Node in id was missing", exception.Message);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenEdgesExistAndCursorsDiffer()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);
            var cursor1 = "cursor1";
            var cursor2 = "cursor2";
            var node = MockNode.Create("node1");
            var relayNode = new RelayEdge<INode>(cursor1, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor1, cursor1));
            var nodeResult = new NodeConnectionResult(null, cursor1, false, false, nodeConnection);
            var edgeConnectionResult = new EdgeConnectionResult(cursor2, cursor2, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

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
                    .Add(childKey, nodeResult)
                    .Add(key, edgeConnectionResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenCursorsMatch()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);
            var cursor = "cursor1";
            var node = MockNode.Create("node1");
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor, cursor));
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);
            var edgeConnectionResult = new EdgeConnectionResult(cursor, cursor, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

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
                    .Add(childKey, nodeResult)
                    .Add(key, edgeConnectionResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenEdgesAreEmpty()
        {
            var mockToEdgeConnectionQueryExecutor = new MockToEdgeConnectionQueryExecutor();
            var executor = new OutToEdgeConnectionQueryExecutor(new EmptyGraphQueryService(), mockToEdgeConnectionQueryExecutor);

            var query = new OutToEdgeConnectionQuery("EdgeType", "NodeInType", "NodeOutType", null, null, ConnectionArguments.Default, 100, false, null);
            var cursor = "cursor1";
            var nodeConnection = Connection<RelayEdge<INode>, INode>.Empty;
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);
            var edgeConnectionResult = new EdgeConnectionResult(cursor, cursor, false, false, Connection<RelayEdge<IEdge>, IEdge>.Empty);

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
                    .Add(childKey, nodeResult)
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

        private sealed record MockToEdgeConnectionQuery(
            string EdgeTypeName,
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
    }
}
