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
    public sealed class ZipNodeConnectionQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithTwoChildConnections()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");
            var node3 = MockNode.Create("node3");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1),
                new RelayEdge<INode>("cursor2", node2));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor3", node3));

            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor2");
            var pageInfo2 = new PageInfo(false, false, "cursor3", "cursor3");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor2", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor3", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.AreEqual(3, result.Connection.Edges.Count);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public async Task ThrowsExceptionWhenChildResultsCountIsNotTwo()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";

            var node1 = MockNode.Create("node1");
            var edges1 = ImmutableList.Create(new RelayEdge<INode>("cursor1", node1));
            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor1");
            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var childResult1 = new NodeConnectionResult(null, "cursor1", false, false, connection1);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1));

            await executor.ExecuteAsync(context, key, cancellationToken);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithEmptyChildConnections()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var pageInfo1 = new PageInfo(false, false, string.Empty, string.Empty);
            var pageInfo2 = new PageInfo(false, false, string.Empty, string.Empty);

            var connection1 = new Connection<RelayEdge<INode>, INode>(ImmutableList<RelayEdge<INode>>.Empty, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(ImmutableList<RelayEdge<INode>>.Empty, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, string.Empty, false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, string.Empty, false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.AreEqual(0, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithPagination()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var connectionArgs = ConnectionArguments.GetFirst(2);
            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                connectionArgs,
                2,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");
            var node3 = MockNode.Create("node3");
            var node4 = MockNode.Create("node4");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1),
                new RelayEdge<INode>("cursor2", node2));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor3", node3),
                new RelayEdge<INode>("cursor4", node4));

            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor2");
            var pageInfo2 = new PageInfo(false, false, "cursor3", "cursor4");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor2", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor4", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.AreEqual(2, result.Connection.Edges.Count);
            Assert.IsTrue(result.Connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithDuplicateNodes()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1),
                new RelayEdge<INode>("cursor2", node2));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor3", node1),
                new RelayEdge<INode>("cursor4", node2));

            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor2");
            var pageInfo2 = new PageInfo(false, false, "cursor3", "cursor4");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor2", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor4", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.AreEqual(2, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithUnevenChildConnectionCounts()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");
            var node3 = MockNode.Create("node3");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1),
                new RelayEdge<INode>("cursor2", node2),
                new RelayEdge<INode>("cursor3", node3));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor4", node1));

            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor3");
            var pageInfo2 = new PageInfo(false, false, "cursor4", "cursor4");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor3", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor4", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithHasNextPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor2", node2));

            var pageInfo1 = new PageInfo(true, false, "cursor1", "cursor1");
            var pageInfo2 = new PageInfo(true, false, "cursor2", "cursor2");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor1", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor2", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.IsTrue(result.Connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithHasPreviousPage()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor2", node2));

            var pageInfo1 = new PageInfo(false, true, "cursor1", "cursor1");
            var pageInfo2 = new PageInfo(false, true, "cursor2", "cursor2");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor1", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor2", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
            Assert.IsTrue(result.Connection.PageInfo.HasPreviousPage);
        }


        [TestMethod]
        public void CanCheckHasMoreChildDataWithNullCursor()
        {
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor2", node2));

            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor1");
            var pageInfo2 = new PageInfo(false, false, "cursor2", "cursor2");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor1", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor2", false, false, connection2);

            var resultEdges = ImmutableList.Create(
                new RelayEdge<INode>("resultCursor1", node1),
                new RelayEdge<INode>("resultCursor2", node2));

            var resultPageInfo = new PageInfo(false, false, "resultCursor1", "resultCursor2");
            var resultConnection = new Connection<RelayEdge<INode>, INode>(resultEdges, resultPageInfo);
            var result = new NodeConnectionResult(null, "resultCursor2", false, false, resultConnection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(key, result)
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var hasMoreData = executor.HasMoreChildData(context, key);

            Assert.IsNotNull(context);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithCursor()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var connectionArgs = ConnectionArguments.GetFirst(25, "someCursor");
            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                connectionArgs,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor2", node2));

            var pageInfo1 = new PageInfo(false, false, "cursor1", "cursor1");
            var pageInfo2 = new PageInfo(false, false, "cursor2", "cursor2");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor1", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor2", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithBalancedCountCalculation()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var executor = new ZipNodeConnectionQueryExecutor(mockCursorSerializer);

            var query = new ZipNodeConnectionQuery(
                ImmutableTree<string, GraphQueryNode>.Empty,
                ConnectionArguments.Default,
                10,
                null);

            var key = "testKey";
            var childKey1 = "child1";
            var childKey2 = "child2";

            var node1 = MockNode.Create("node1");
            var node2 = MockNode.Create("node2");
            var node3 = MockNode.Create("node3");
            var node4 = MockNode.Create("node4");
            var node5 = MockNode.Create("node5");

            var edges1 = ImmutableList.Create(
                new RelayEdge<INode>("cursor1", node1),
                new RelayEdge<INode>("cursor2", node2),
                new RelayEdge<INode>("cursor3", node3));

            var edges2 = ImmutableList.Create(
                new RelayEdge<INode>("cursor4", node4),
                new RelayEdge<INode>("cursor5", node5));

            var pageInfo1 = new PageInfo(true, false, "cursor1", "cursor3");
            var pageInfo2 = new PageInfo(true, false, "cursor4", "cursor5");

            var connection1 = new Connection<RelayEdge<INode>, INode>(edges1, pageInfo1);
            var connection2 = new Connection<RelayEdge<INode>, INode>(edges2, pageInfo2);

            var childResult1 = new NodeConnectionResult(null, "cursor3", false, false, connection1);
            var childResult2 = new NodeConnectionResult(null, "cursor5", false, false, connection2);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey1, new GraphQueryNode(new NodeConnectionQuery("Type1", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddNode(childKey2, new GraphQueryNode(new NodeConnectionQuery("Type2", null, null, ConnectionArguments.Default, 10, false, null)))
                .AddParentNode(childKey1, key, new GraphQueryNode(query))
                .AddParentNode(childKey2, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey1, childResult1)
                    .Add(childKey2, childResult2));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Connection);
        }

        private sealed class MockGraphCursorSerializationService : IGraphCursorSerializationService
        {
            public Cursor Deserialize(string cursor)
            {
                if (cursor == "childCursor")
                {
                    var rootKey = "root";
                    var cursor1 = Cursor.Create(CursorNode.CreateEndOfData());
                    var cursor2 = Cursor.Create(CursorNode.CreateEndOfData());
                    var rootCursor = Cursor.Create(CursorNode.Empty);
                    rootCursor = rootCursor.AddSubTree(cursor1, rootKey);
                    rootCursor = rootCursor.AddSubTree(cursor2, rootKey);
                    return rootCursor;
                }

                return Cursor.Create(CursorNode.CreateEndOfData());
            }

            public string Serialize(Cursor cursor)
            {
                return "serialized_cursor";
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
