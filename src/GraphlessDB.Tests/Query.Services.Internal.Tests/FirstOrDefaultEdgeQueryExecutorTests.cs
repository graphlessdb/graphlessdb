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
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class FirstOrDefaultEdgeQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithSingleEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FirstOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var edge = MockEdge.Create(edgeId, "inNode", "outNode");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [relayEdge],
                new PageInfo(false, true, string.Empty, cursor));

            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Edge);
            Assert.AreEqual(cursor, result.Edge.Cursor);
            Assert.AreEqual(cursor, result.Cursor);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNoEdgesAndNoMorePages()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FirstOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [],
                new PageInfo(false, false, string.Empty, string.Empty));

            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(edgeConnectionResult, key, childKey);

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
        public async Task CanExecuteAsyncWithNoEdgesButHasNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FirstOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [],
                new PageInfo(true, false, string.Empty, string.Empty));

            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(edgeConnectionResult, key, childKey);

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
        public void CanCheckHasMoreChildDataWithNonEmptyEdgesAndMatchingCursor()
        {
            var cursor = "cursor1";

            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FirstOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var edge = MockEdge.Create("edge1", "inNode", "outNode");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [relayEdge],
                new PageInfo(false, true, string.Empty, cursor));

            var edgeResult = new EdgeResult(cursor, cursor, false, false, relayEdge);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new EdgeByIdQuery("Edge", "in", "out", false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(new FirstOrDefaultEdgeQuery(null)));

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
        public void CanCheckHasMoreChildDataWithNonEmptyEdgesAndDifferentCursor()
        {
            var cursor1 = "cursor1";
            var cursor2 = "cursor2";

            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FirstOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var edge = MockEdge.Create("edge1", "inNode", "outNode");
            var relayEdge = new RelayEdge<IEdge>(cursor1, edge);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [relayEdge],
                new PageInfo(false, true, string.Empty, cursor2));

            var edgeResult = new EdgeResult(null, cursor1, false, false, relayEdge);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor1, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new EdgeByIdQuery("Edge", "in", "out", false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(new FirstOrDefaultEdgeQuery(null)));

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
        public void CanCheckHasMoreChildDataWithEmptyEdges()
        {
            var mockCursorSerializer = new MockGraphCursorSerializationService();

            var executor = new FirstOrDefaultEdgeQueryExecutor(mockCursorSerializer);

            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [],
                new PageInfo(false, false, string.Empty, string.Empty));

            var edgeResult = new EdgeResult(null, "serialized_cursor", false, false, null);
            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new EdgeByIdQuery("Edge", "in", "out", false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(new FirstOrDefaultEdgeQuery(null)));

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
            EdgeConnectionResult edgeConnectionResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new EdgeByIdQuery("Edge", "in", "out", false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(new FirstOrDefaultEdgeQuery(null)));

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
