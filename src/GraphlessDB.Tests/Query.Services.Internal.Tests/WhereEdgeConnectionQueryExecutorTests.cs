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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class WhereEdgeConnectionQueryExecutorTests
    {
        private static CancellationToken GetCancellationToken()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
        }

        private static string CreateCursor(GraphCursorSerializationService cursorSerializer, string id)
        {
            return cursorSerializer.Serialize(Cursor.Create(new CursorNode(
                new HasTypeCursor(id, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty),
                null, null, null, null, null, null, null)));
        }

        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithFilterPredicate()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var query = new WhereEdgeConnectionQuery(
                ctx => Task.FromResult(((MockEdge)ctx.Item.Node).OutId == "node2"),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
            Assert.AreEqual("node2", ((MockEdge)result.Connection.Edges.First().Node).OutId);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithEmptyConnection()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var edges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(string.Empty, string.Empty, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithPagination()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var connectionArgs = ConnectionArguments.GetFirst(1);
            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                connectionArgs,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
            Assert.IsTrue(result.Connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithAfterCursor()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData()));
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var connectionArgs = ConnectionArguments.GetFirst(10, cursor1);
            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                connectionArgs,
                100,
                false,
                null);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithChildHasNextPage()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edges = ImmutableList.Create(relayEdge1);
            var pageInfo = new PageInfo(true, false, cursor1, cursor1);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor1, cursor1, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithChildHasPreviousPage()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edges = ImmutableList.Create(relayEdge1);
            var pageInfo = new PageInfo(false, true, cursor1, cursor1);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor1, cursor1, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Connection.PageInfo.HasPreviousPage);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithFilterExcludingAllEdges()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(false),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(true, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
            Assert.IsTrue(result.Connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanExecuteAsyncNeedsMoreData()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var connectionArgs = ConnectionArguments.GetFirst(10);
            var query = new WhereEdgeConnectionQuery(
                ctx => Task.FromResult(((MockEdge)ctx.Item.Node).OutId == "node2"),
                connectionArgs,
                100,
                false,
                null);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(true, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.NeedsMoreData);
        }

        [TestMethod]
        public void CanCheckHasMoreChildData()
        {
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edges = ImmutableList.Create(relayEdge1);
            var pageInfo = new PageInfo(false, false, cursor1, cursor1);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor1, cursor1, false, false, connection);
            var result = new EdgeConnectionResult(cursor1, cursor1, false, false, connection);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, result)
                    .Add(childKey, childResult));

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsFalse(hasMoreData);
        }

        [TestMethod]
        public void CanCheckHasMoreChildDataWithDifferentCursors()
        {
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var childEdges = ImmutableList.Create(relayEdge1, relayEdge2);
            var childPageInfo = new PageInfo(false, false, cursor1, cursor2);
            var childConnection = new Connection<RelayEdge<IEdge>, IEdge>(childEdges, childPageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, childConnection);

            var resultEdges = ImmutableList.Create(relayEdge1);
            var resultPageInfo = new PageInfo(false, false, cursor1, cursor1);
            var resultConnection = new Connection<RelayEdge<IEdge>, IEdge>(resultEdges, resultPageInfo);
            var result = new EdgeConnectionResult(cursor1, cursor1, false, false, resultConnection);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, result)
                    .Add(childKey, childResult));

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsTrue(hasMoreData);
        }

        [TestMethod]
        public void CanCheckHasMoreChildDataWithEmptyChildConnection()
        {
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var childEdges = ImmutableList<RelayEdge<IEdge>>.Empty;
            var childPageInfo = new PageInfo(false, false, string.Empty, string.Empty);
            var childConnection = new Connection<RelayEdge<IEdge>, IEdge>(childEdges, childPageInfo);
            var childResult = new EdgeConnectionResult(string.Empty, string.Empty, false, false, childConnection);

            var result = new EdgeConnectionResult(string.Empty, string.Empty, false, false, childConnection);

            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                ConnectionArguments.Default,
                100,
                false,
                null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, result)
                    .Add(childKey, childResult));

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsFalse(hasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNullAfterCursor()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var connectionArgs = new ConnectionArguments(10, null, null, null);
            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                connectionArgs,
                100,
                false,
                null);

            var edges = ImmutableList.Create(relayEdge1);
            var pageInfo = new PageInfo(false, false, cursor1, cursor1);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor1, cursor1, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithWhitespaceAfterCursor()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursor1 = CreateCursor(cursorSerializer, "edge1");
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var connectionArgs = new ConnectionArguments(10, "   ", null, null);
            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                connectionArgs,
                100,
                false,
                null);

            var edges = ImmutableList.Create(relayEdge1);
            var pageInfo = new PageInfo(false, false, cursor1, cursor1);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor1, cursor1, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithCursorHavingChildNode()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var childCursorNode = CursorNode.CreateEndOfData();
            var parentCursorNode = new CursorNode(
                new HasTypeCursor("edge1", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty),
                null, null, null, null, null, null, null);
            var cursor1 = cursorSerializer.Serialize(new Cursor(
                ImmutableTree<string, CursorNode>.Empty
                    .AddNode("root", parentCursorNode)
                    .AddParentNode("root", "child", childCursorNode)));
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var connectionArgs = ConnectionArguments.GetFirst(10, cursor1);
            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                connectionArgs,
                100,
                false,
                null);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithCursorHavingNoChildNodes()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var executor = new WhereEdgeConnectionQueryExecutor(cursorSerializer);

            var edge1 = MockEdge.Create("node1", "node2");
            var cursorNode = new CursorNode(
                new HasTypeCursor("edge1", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty),
                null, null, null, null, null, null, null);
            var cursor1 = cursorSerializer.Serialize(new Cursor(
                ImmutableTree<string, CursorNode>.Empty.AddNode("root", cursorNode)));
            var relayEdge1 = new RelayEdge<IEdge>(cursor1, edge1);

            var edge2 = MockEdge.Create("node1", "node3");
            var cursor2 = CreateCursor(cursorSerializer, "edge2");
            var relayEdge2 = new RelayEdge<IEdge>(cursor2, edge2);

            var connectionArgs = ConnectionArguments.GetFirst(10, cursor1);
            var query = new WhereEdgeConnectionQuery(
                _ => Task.FromResult(true),
                connectionArgs,
                100,
                false,
                null);

            var edges = ImmutableList.Create(relayEdge1, relayEdge2);
            var pageInfo = new PageInfo(false, false, cursor1, cursor2);
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(edges, pageInfo);
            var childResult = new EdgeConnectionResult(cursor2, cursor2, false, false, connection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new MockFromEdgeConnectionQuery()))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, childResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        private sealed record MockFromEdgeConnectionQuery()
            : FromEdgeConnectionQuery(null, false, null);

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
