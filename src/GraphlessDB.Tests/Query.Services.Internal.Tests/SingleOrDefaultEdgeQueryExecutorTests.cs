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
    public sealed class SingleOrDefaultEdgeQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithSingleEdgeAndNoNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var query = new SingleOrDefaultEdgeQuery(null);
            var edge = MockEdge.Create(edgeId, "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var pageInfo = new PageInfo(false, false, cursor, cursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList.Create(relayEdge), pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Edge);
            Assert.AreEqual(cursor, result.Edge.Cursor);
            Assert.AreEqual(edge, result.Edge.Node);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNoEdgesAndNoNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var cursor = "cursor1";

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var query = new SingleOrDefaultEdgeQuery(null);
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Edge);
            Assert.IsNull(result.ChildCursor);
            Assert.AreEqual("serialized_cursor", result.Cursor);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public async Task ThrowsExceptionWhenMoreThanOneEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var cursor1 = "cursor1";
            var cursor2 = "cursor2";

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var query = new SingleOrDefaultEdgeQuery(null);
            var edge1 = MockEdge.Create("edge1", "node1", "node2");
            var edge2 = MockEdge.Create("edge2", "node1", "node3");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList.Create(relayEdge1, relayEdge2), pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor2, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            await executor.ExecuteAsync(context, key, cancellationToken);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithSingleEdgeAndHasNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var query = new SingleOrDefaultEdgeQuery(null);
            var edge = MockEdge.Create(edgeId, "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var pageInfo = new PageInfo(true, false, cursor, cursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList.Create(relayEdge), pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Edge);
            Assert.IsNull(result.ChildCursor);
            Assert.AreEqual(string.Empty, result.Cursor);
            Assert.IsTrue(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNoEdgesAndHasNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var cursor = "cursor1";

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var query = new SingleOrDefaultEdgeQuery(null);
            var pageInfo = new PageInfo(true, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Edge);
            Assert.IsNull(result.ChildCursor);
            Assert.AreEqual(string.Empty, result.Cursor);
            Assert.IsTrue(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenEdgesExistAndCursorsDoNotMatch()
        {
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var edgeId = "edge1";
            var cursor1 = "cursor1";
            var cursor2 = "cursor2";

            var query = new SingleOrDefaultEdgeQuery(null);
            var edge = MockEdge.Create(edgeId, "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>(cursor1, edge);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList.Create(relayEdge), pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor2, false, false, connection);

            var edgeResult = new EdgeResult(cursor1, cursor1, false, false, relayEdge);

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, edgeConnectionResult)
                    .Add(key, edgeResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenEdgesExistAndCursorsMatch()
        {
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var edgeId = "edge1";
            var cursor = "cursor1";

            var query = new SingleOrDefaultEdgeQuery(null);
            var edge = MockEdge.Create(edgeId, "node1", "node2");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var pageInfo = new PageInfo(false, false, cursor, cursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList.Create(relayEdge), pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var edgeResult = new EdgeResult(cursor, cursor, false, false, relayEdge);

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, edgeConnectionResult)
                    .Add(key, edgeResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenNoEdges()
        {
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new SingleOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var cursor = "cursor1";

            var query = new SingleOrDefaultEdgeQuery(null);
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(ImmutableList<RelayEdge<IEdge>>.Empty, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var edgeResult = new EdgeResult(null, cursor, false, false, null);

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, edgeConnectionResult)
                    .Add(key, edgeResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        private static GraphExecutionContext CreateContextWithEdgeConnectionResult(
            SingleOrDefaultEdgeQuery query,
            EdgeConnectionResult edgeConnectionResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            return new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeConnectionResult));
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
