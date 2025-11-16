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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class FromEdgeQueryExecutorTests
    {
        [TestMethod]
        public async Task CanExecuteAsyncWithSingleNodeFromInEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var targetNodeId = "targetNode1";
            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockQueryService = new MockGraphQueryService();
            var mockNode = MockNode.Create(targetNodeId);
            mockQueryService.AddNode(targetNodeId, mockNode);

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new InFromEdgeQuery(false, null);
            var edge = MockEdge.Create(edgeId, targetNodeId, "otherNode");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgeResult = new EdgeResult(null, cursor, false, false, relayEdge);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeResult(query, edgeResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, e => e.InId, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Node);
            Assert.AreEqual(targetNodeId, result.Node.Node.Id);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithSingleNodeFromOutEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var targetNodeId = "targetNode1";
            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockQueryService = new MockGraphQueryService();
            var mockNode = MockNode.Create(targetNodeId);
            mockQueryService.AddNode(targetNodeId, mockNode);

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new OutFromEdgeQuery(false, null);
            var edge = MockEdge.Create(edgeId, "otherNode", targetNodeId);
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgeResult = new EdgeResult(null, cursor, false, false, relayEdge);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeResult(query, edgeResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, e => e.OutId, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Node);
            Assert.AreEqual(targetNodeId, result.Node.Node.Id);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithDanglingEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var targetNodeId = "targetNode1";
            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockQueryService = new MockGraphQueryService();
            // Don't add the node - this creates a dangling edge

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new InFromEdgeQuery(false, null);
            var edge = MockEdge.Create(edgeId, targetNodeId, "otherNode");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgeResult = new EdgeResult(null, cursor, false, false, relayEdge);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeResult(query, edgeResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, e => e.InId, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Node);
            Assert.IsTrue(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
            Assert.IsTrue(mockLogger.WasDanglingEdgeLogged);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithConsistentRead()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var targetNodeId = "targetNode1";
            var edgeId = "edge1";
            var cursor = "cursor1";

            var mockQueryService = new MockGraphQueryService();
            var mockNode = MockNode.Create(targetNodeId);
            mockQueryService.AddNode(targetNodeId, mockNode);

            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new InFromEdgeQuery(true, null);
            var edge = MockEdge.Create(edgeId, targetNodeId, "otherNode");
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);
            var edgeResult = new EdgeResult(null, cursor, false, false, relayEdge);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeResult(query, edgeResult, key, childKey);

            await executor.ExecuteAsync(context, key, e => e.InId, cancellationToken);

            Assert.IsTrue(mockQueryService.LastConsistentRead);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task ThrowsExceptionWhenParentHasNoChildren()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockQueryService = new MockGraphQueryService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new InFromEdgeQuery(false, null);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            await executor.ExecuteAsync(context, key, e => e.InId, cancellationToken);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public async Task ThrowsExceptionWhenChildResultIsNull()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var mockQueryService = new MockGraphQueryService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new InFromEdgeQuery(false, null);

            var childKey = "childKey";
            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new EdgeByIdQuery("Edge", "in", "out", false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            await executor.ExecuteAsync(context, key, e => e.InId, cancellationToken);
        }

        [TestMethod]
        public async Task CanExecuteAsyncWithNullEdge()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var cursor = "cursor1";

            var mockQueryService = new MockGraphQueryService();
            var mockCursorSerializer = new MockGraphCursorSerializationService();
            var mockLogger = new MockLogger<FromEdgeQueryExecutor>();

            var executor = new FromEdgeQueryExecutor(
                mockQueryService,
                mockCursorSerializer,
                mockLogger);

            var query = new InFromEdgeQuery(false, null);
            var edgeResult = new EdgeResult(null, cursor, false, false, null);

            var childKey = "childKey";
            var key = "testKey";
            var context = CreateContextWithEdgeResult(query, edgeResult, key, childKey);

            var resultContext = await executor.ExecuteAsync(context, key, e => e.InId, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.GetResult<NodeResult>(key);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Node);
            Assert.IsTrue(result.NeedsMoreData);
        }

        private static GraphExecutionContext CreateContextWithEdgeResult(
            FromEdgeQuery query,
            EdgeResult edgeResult,
            string key,
            string childKey)
        {
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new EdgeByIdQuery("Edge", "in", "out", false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            return new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty.Add(childKey, edgeResult));
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            private readonly System.Collections.Generic.Dictionary<string, INode> nodes = new();

            public bool LastConsistentRead { get; private set; }

            public void AddNode(string id, INode node)
            {
                nodes[id] = node;
            }

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
                throw new NotImplementedException();
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
                LastConsistentRead = request.ConsistentRead;

                var resultNodes = request.Ids
                    .Select(id => nodes.TryGetValue(id, out var node)
                        ? new RelayEdge<INode>("cursor_" + id, node)
                        : null)
                    .ToImmutableList();

                return Task.FromResult(new TryGetNodesResponse(resultNodes));
            }

            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(
                TryGetVersionedNodesRequest request,
                CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
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

        private sealed class MockLogger<T> : ILogger<T>
        {
            public bool WasDanglingEdgeLogged { get; private set; }

            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull
            {
                return null;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                var message = formatter(state, exception);
                if (message.Contains("Dangling"))
                {
                    WasDanglingEdgeLogged = true;
                }
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
