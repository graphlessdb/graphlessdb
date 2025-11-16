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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class FirstEdgeQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithSingleEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var inId = "node1";
            var outId = "node2";
            var cursor = "cursor1";

            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var edge = MockEdge.Create(inId, outId);
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, cursor, cursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Edge);
            Assert.AreEqual(inId, result.Edge.Node.InId);
            Assert.AreEqual(outId, result.Edge.Node.OutId);
            Assert.AreEqual(cursor, result.ChildCursor);
            Assert.AreEqual(cursor, result.Cursor);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithMultipleEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var inId1 = "node1";
            var outId1 = "node2";
            var inId2 = "node3";
            var outId2 = "node4";
            var cursor1 = "cursor1";
            var cursor2 = "cursor2";

            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var edge1 = MockEdge.Create(inId1, outId1);
            var edge2 = MockEdge.Create(inId2, outId2);
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge1).Add(relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor1, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Edge);
            Assert.AreEqual(inId1, result.Edge.Node.InId);
            Assert.AreEqual(outId1, result.Edge.Node.OutId);
            Assert.AreEqual(cursor1, result.ChildCursor);
            Assert.AreEqual(cursor1, result.Cursor);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNoEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, "cursor", false, false, connection);

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
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNoEdgesButHasNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(true, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, "cursor", false, false, connection);

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
        public void HasMoreChildDataReturnsFalseWhenEdgesAreEmpty()
        {
            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, "cursor", false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var edgeResult = new EdgeResult(null, "cursor", false, false, null);
            var context = CreateContextWithEdgeConnectionResultAndEdgeResult(
                query, edgeConnectionResult, edgeResult, key, childKey);

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenChildCursorEqualsEndCursor()
        {
            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var inId = "node1";
            var outId = "node2";
            var cursor = "cursor1";
            var edge = MockEdge.Create(inId, outId);
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, cursor, cursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var edgeResult = new EdgeResult(cursor, cursor, false, false, relayEdge);
            var context = CreateContextWithEdgeConnectionResultAndEdgeResult(
                query, edgeConnectionResult, edgeResult, key, childKey);

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenChildCursorNotEqualsEndCursor()
        {
            var executor = new FirstEdgeQueryExecutor();

            var query = new FirstEdgeQuery(null);
            var inId = "node1";
            var outId = "node2";
            var cursor1 = "cursor1";
            var cursor2 = "cursor2";
            var edge = MockEdge.Create(inId, outId);
            var relayEdge = new RelayEdge<IEdge>(cursor1, edge);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor1, false, false, connection);

            var childKey = "childKey";
            var key = "testKey";
            var edgeResult = new EdgeResult(cursor1, cursor1, false, false, relayEdge);
            var context = CreateContextWithEdgeConnectionResultAndEdgeResult(
                query, edgeConnectionResult, edgeResult, key, childKey);

            var result = executor.HasMoreChildData(context, key);

            Assert.IsTrue(result);
        }

        private static GraphExecutionContext CreateContextWithEdgeConnectionResult(
            FirstEdgeQuery query,
            EdgeConnectionResult edgeConnectionResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            return new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeConnectionResult));
        }

        private static GraphExecutionContext CreateContextWithEdgeConnectionResultAndEdgeResult(
            FirstEdgeQuery query,
            EdgeConnectionResult edgeConnectionResult,
            EdgeResult edgeResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            return new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, edgeConnectionResult)
                    .Add(key, edgeResult));
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
    }
}
