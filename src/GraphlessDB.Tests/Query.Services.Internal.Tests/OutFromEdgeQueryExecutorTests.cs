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
    public sealed class OutFromEdgeQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var nodeId = "node1";
            var cursor = "cursor1";
            var key = "testKey";

            var mockFromEdgeQueryExecutor = new MockFromEdgeQueryExecutor();
            var executor = new OutFromEdgeQueryExecutor(mockFromEdgeQueryExecutor);

            var query = new OutFromEdgeQuery(false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeResult = new NodeResult(cursor, cursor, false, false, relayNode);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            mockFromEdgeQueryExecutor.SetResult(context.SetResult(key, nodeResult));

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.AreEqual(context, mockFromEdgeQueryExecutor.LastContext);
            Assert.AreEqual(key, mockFromEdgeQueryExecutor.LastKey);
            Assert.IsNotNull(mockFromEdgeQueryExecutor.LastGetTargetId);
            Assert.AreEqual(cancellationToken, mockFromEdgeQueryExecutor.LastCancellationToken);

            // Verify the lambda extracts OutId
            var mockEdge = MockEdge.Create("inId1", "outId1");
            var extractedId = mockFromEdgeQueryExecutor.LastGetTargetId!(mockEdge);
            Assert.AreEqual("outId1", extractedId);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsTrueWhenEdgeExistsAndNodeIsNull()
        {
            var mockFromEdgeQueryExecutor = new MockFromEdgeQueryExecutor();
            var executor = new OutFromEdgeQueryExecutor(mockFromEdgeQueryExecutor);

            var query = new OutFromEdgeQuery(false, null);
            var cursor = "cursor1";
            var edge = MockEdge.Create("inId1", "outId1");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgeResult = new EdgeResult(cursor, cursor, false, false, relayEdge);
            var nodeResult = new NodeResult(null, cursor, false, false, null);

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
                    .Add(childKey, edgeResult)
                    .Add(key, nodeResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenNodeExists()
        {
            var mockFromEdgeQueryExecutor = new MockFromEdgeQueryExecutor();
            var executor = new OutFromEdgeQueryExecutor(mockFromEdgeQueryExecutor);

            var query = new OutFromEdgeQuery(false, null);
            var cursor = "cursor1";
            var edge = MockEdge.Create("inId1", "outId1");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgeResult = new EdgeResult(cursor, cursor, false, false, relayEdge);
            var node = MockNode.Create("node1");
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeResult = new NodeResult(cursor, cursor, false, false, relayNode);

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
                    .Add(childKey, edgeResult)
                    .Add(key, nodeResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalseWhenEdgeIsNull()
        {
            var mockFromEdgeQueryExecutor = new MockFromEdgeQueryExecutor();
            var executor = new OutFromEdgeQueryExecutor(mockFromEdgeQueryExecutor);

            var query = new OutFromEdgeQuery(false, null);
            var cursor = "cursor1";
            var edgeResult = new EdgeResult(null, cursor, false, false, null);
            var nodeResult = new NodeResult(null, cursor, false, false, null);

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
                    .Add(childKey, edgeResult)
                    .Add(key, nodeResult));

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        private sealed class MockFromEdgeQueryExecutor : IFromEdgeQueryExecutor
        {
            private GraphExecutionContext? _result;
            public GraphExecutionContext? LastContext { get; private set; }
            public string? LastKey { get; private set; }
            public Func<IEdge, string>? LastGetTargetId { get; private set; }
            public CancellationToken LastCancellationToken { get; private set; }

            public void SetResult(GraphExecutionContext result)
            {
                _result = result;
            }

            public Task<GraphExecutionContext> ExecuteAsync(
                GraphExecutionContext context,
                string key,
                Func<IEdge, string> getTargetId,
                CancellationToken cancellationToken)
            {
                LastContext = context;
                LastKey = key;
                LastGetTargetId = getTargetId;
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
