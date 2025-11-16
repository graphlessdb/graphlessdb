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
    public sealed class SingleEdgeQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithSingleEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var executor = new SingleEdgeQueryExecutor();

            var cursor = "cursor1";
            var edge = MockEdge.Create("node1", "node2");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);

            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, string.Empty, cursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, cursor, false, false, connection);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Edge);
            Assert.AreEqual(cursor, result.Edge.Cursor);
            Assert.AreEqual("node1", result.Edge.Node.InId);
            Assert.AreEqual("node2", result.Edge.Node.OutId);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public async Task ThrowsExceptionWhenMoreThanOneEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var executor = new SingleEdgeQueryExecutor();

            var edge1 = MockEdge.Create("node1", "node2");
            var edge2 = MockEdge.Create("node3", "node4");
            var relayEdge1 = new RelayEdge<IEdge>("cursor1", edge1);
            var relayEdge2 = new RelayEdge<IEdge>("cursor2", edge2);

            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge1).Add(relayEdge2);
            var pageInfo = new PageInfo(false, false, string.Empty, "cursor2");
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, "cursor2", false, false, connection);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            await executor.ExecuteAsync(context, key, cancellationToken);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithEmptyConnectionAndHasNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var executor = new SingleEdgeQueryExecutor();

            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(true, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Edge);
            Assert.IsTrue(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithEmptyConnectionAndNoNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var executor = new SingleEdgeQueryExecutor();

            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeConnectionResult(query, edgeConnectionResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<EdgeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Edge);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public void CanCheckHasMoreChildDataWhenNoCursorMatch()
        {
            var executor = new SingleEdgeQueryExecutor();

            var edge = MockEdge.Create("node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge);
            var pageInfo = new PageInfo(false, false, string.Empty, "endCursor");
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var edgeResult = new EdgeResult("differentCursor", "cursor1", false, false, relayEdge);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithResults(query, edgeResult, edgeConnectionResult, key, childKey);

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsTrue(hasMoreData);
        }

        [TestMethod]
        public void CanCheckHasMoreChildDataWhenCursorMatches()
        {
            var executor = new SingleEdgeQueryExecutor();

            var edge = MockEdge.Create("node1", "node2");
            var relayEdge = new RelayEdge<IEdge>("cursor1", edge);
            var edges = ImmutableList<RelayEdge<IEdge>>.Empty.Add(relayEdge);
            var endCursor = "endCursor";
            var pageInfo = new PageInfo(false, false, string.Empty, endCursor);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var edgeResult = new EdgeResult(endCursor, "cursor1", false, false, relayEdge);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithResults(query, edgeResult, edgeConnectionResult, key, childKey);

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsFalse(hasMoreData);
        }

        [TestMethod]
        public void CanCheckHasMoreChildDataWhenEmptyConnection()
        {
            var executor = new SingleEdgeQueryExecutor();

            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var edgeConnectionResult = new EdgeConnectionResult(null, string.Empty, false, false, connection);

            var edgeResult = new EdgeResult(null, string.Empty, false, false, null);

            var query = new SingleEdgeQuery(null);
            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithResults(query, edgeResult, edgeConnectionResult, key, childKey);

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsFalse(hasMoreData);
        }

        private static GraphExecutionContext CreateContextWithEdgeConnectionResult(
            SingleEdgeQuery query,
            EdgeConnectionResult edgeConnectionResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 1, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            return new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeConnectionResult));
        }

        private static GraphExecutionContext CreateContextWithResults(
            SingleEdgeQuery query,
            EdgeResult edgeResult,
            EdgeConnectionResult edgeConnectionResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereEdgeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 1, false, null)))
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
