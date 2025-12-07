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
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Graph.Services.Internal.Tests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class NodeConnectionQueryExecutorTests
    {
        private static CancellationToken GetCancellationToken()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
        }

        [TestMethod]
        public async Task CanAsync()
        {
            var cancellationToken = GetCancellationToken();

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executor = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryNodeExecutionService<NodeConnectionQuery>>();

            var key = string.Empty;
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, true, null)));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
        }

        [TestMethod]
        public void HasMoreChildDataReturnsFalse()
        {
            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var key = "testKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, true, null)));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var result = executor.HasMoreChildData(context, key);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithNonRootCursorRestoresFromCursor()
        {
            var cancellationToken = GetCancellationToken();
            var nodeId = "node123";
            var childKey = "childKey";
            var parentKey = "parentKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var node = MockNode.Create(nodeId);
            graphQueryService.SetNode(nodeId, node);

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, cursor, null, null), 25, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(query))
                .AddParentNode(childKey, parentKey, new GraphQueryNode(new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null)));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, childKey, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<NodeConnectionResult>(childKey);
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Connection.Edges.Count);
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

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            private readonly Dictionary<string, INode> nodes = new();
            private Connection<RelayEdge<INode>, INode>? connectionByTypeResponse;
            private Connection<RelayEdge<INode>, INode>? connectionByTypeAndPropertyNameResponse;
            private Connection<RelayEdge<INode>, INode>? connectionByTypePropertyNameAndValueResponse;
            private Connection<RelayEdge<INode>, INode>? connectionByTypePropertyNameAndValuesResponse;
            private Func<Connection<RelayEdge<INode>, INode>>? connectionCallback;

            public bool GetConnectionByTypeAndPropertyNameAsyncCalled { get; private set; }
            public bool GetConnectionByTypePropertyNameAndValueAsyncCalled { get; private set; }
            public bool GetConnectionByTypePropertyNameAndValuesAsyncCalled { get; private set; }

            public void SetNode(string id, INode node)
            {
                nodes[id] = node;
            }

            public void SetConnectionByTypeResponse(Connection<RelayEdge<INode>, INode> connection)
            {
                connectionByTypeResponse = connection;
            }

            public void SetConnectionByTypeAndPropertyNameResponse(Connection<RelayEdge<INode>, INode> connection)
            {
                connectionByTypeAndPropertyNameResponse = connection;
            }

            public void SetConnectionByTypePropertyNameAndValueResponse(Connection<RelayEdge<INode>, INode> connection)
            {
                connectionByTypePropertyNameAndValueResponse = connection;
            }

            public void SetConnectionByTypePropertyNameAndValuesResponse(Connection<RelayEdge<INode>, INode> connection)
            {
                connectionByTypePropertyNameAndValuesResponse = connection;
            }

            public void SetConnectionCallback(Func<Connection<RelayEdge<INode>, INode>> callback)
            {
                connectionCallback = callback;
            }

            public Task ClearAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
            {
                var cursorSerializer = new GraphCursorSerializationService();
                var foundNodes = request.Ids
                    .Select(id =>
                    {
                        if (nodes.TryGetValue(id, out var node))
                        {
                            var hasTypeCursor = new HasTypeCursor(id, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
                            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
                            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));
                            return (RelayEdge<INode>?)new RelayEdge<INode>(cursor, node);
                        }
                        return null;
                    })
                    .ToImmutableList();
                return Task.FromResult(new TryGetNodesResponse(foundNodes));
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
                var response = connectionCallback?.Invoke() ?? connectionByTypeResponse ?? Connection<RelayEdge<INode>, INode>.Empty;
                return Task.FromResult(new GetConnectionResponse(response));
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(GetConnectionByTypeAndPropertyNameRequest request, CancellationToken cancellationToken)
            {
                GetConnectionByTypeAndPropertyNameAsyncCalled = true;
                return Task.FromResult(new GetConnectionResponse(connectionByTypeAndPropertyNameResponse ?? Connection<RelayEdge<INode>, INode>.Empty));
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(GetConnectionByTypePropertyNameAndValueRequest request, CancellationToken cancellationToken)
            {
                GetConnectionByTypePropertyNameAndValueAsyncCalled = true;
                return Task.FromResult(new GetConnectionResponse(connectionByTypePropertyNameAndValueResponse ?? Connection<RelayEdge<INode>, INode>.Empty));
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(GetConnectionByTypePropertyNameAndValuesRequest request, CancellationToken cancellationToken)
            {
                GetConnectionByTypePropertyNameAndValuesAsyncCalled = true;
                return Task.FromResult(new GetConnectionResponse(connectionByTypePropertyNameAndValuesResponse ?? Connection<RelayEdge<INode>, INode>.Empty));
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

        private sealed class MockNodeFilterService : IGraphNodeFilterService
        {
            private bool isPostFilteringRequired;
            private NodePushdownQueryData? pushdownQueryData;

            public void SetPostFilteringRequired(bool value)
            {
                isPostFilteringRequired = value;
            }

            public void SetPushdownQueryData(NodePushdownQueryData? data)
            {
                pushdownQueryData = data;
            }

            public bool IsPostFilteringRequired(INodeFilter? filter)
            {
                return isPostFilteringRequired;
            }

            public NodePushdownQueryData? TryGetNodePushdownQueryData(
                string typeName,
                INodeFilter? filter,
                INodeOrder? order,
                CancellationToken cancellationToken)
            {
                return pushdownQueryData;
            }

            public Task<bool> IsFilterMatchAsync(
                INode node,
                INodeFilter? filter,
                bool consistentRead,
                CancellationToken cancellationToken)
            {
                return Task.FromResult(true);
            }

            public static Task<Connection<RelayEdge<INode>, INode>> GetFilteredNodeConnectionAsync(
                Connection<RelayEdge<INode>, INode> connection,
                INodeFilter? filter,
                bool consistentRead,
                CancellationToken cancellationToken)
            {
                return Task.FromResult(connection);
            }
        }

        [TestMethod]
        public async Task ExecuteAsyncWithCursorContainingChildNodesThrowsException()
        {
            var cancellationToken = GetCancellationToken();
            var nodeId = "node123";
            var childKey = "childKey";
            var parentKey = "parentKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var baseCursor = Cursor.Create(cursorNode);
            var childCursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursorWithChild = baseCursor.AddChildNode(childCursorNode, baseCursor.GetRootKey(), out _);
            var cursor = cursorSerializer.Serialize(cursorWithChild);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, cursor, null, null), 25, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(query))
                .AddParentNode(childKey, parentKey, new GraphQueryNode(new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null)));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(
                async () => await executor.ExecuteAsync(context, childKey, cancellationToken));
        }

        [TestMethod]
        public async Task ExecuteAsyncWithCursorMissingSubjectThrowsException()
        {
            var cancellationToken = GetCancellationToken();
            var childKey = "childKey";
            var parentKey = "parentKey";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, cursor, null, null), 25, false, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(query))
                .AddParentNode(childKey, parentKey, new GraphQueryNode(new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null)));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(
                async () => await executor.ExecuteAsync(context, childKey, cancellationToken));
        }

        [TestMethod]
        public async Task ExecuteAsyncTruncatesResultsWhenDeltaCountExceedsPageSize()
        {
            var cancellationToken = GetCancellationToken();
            var key = string.Empty;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var edges = new List<RelayEdge<INode>>();
            for (int i = 0; i < 15; i++)
            {
                var node = MockNode.Create($"node{i}");
                var hasTypeCursor = new HasTypeCursor($"node{i}", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
                var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
                var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));
                edges.Add(new RelayEdge<INode>(cursor, node));
            }

            var connection = new Connection<RelayEdge<INode>, INode>(
                edges.ToImmutableList(),
                new PageInfo(true, false, edges[0].Cursor, edges[^1].Cursor));

            graphQueryService.SetConnectionByTypeResponse(connection);

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, null, null, null), 25, true, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(10, result.Connection.Edges.Count);
            Assert.IsTrue(result.Connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task ExecuteAsyncTruncatesFinalResultForRootQueryWhenOverFetching()
        {
            var cancellationToken = GetCancellationToken();
            var key = string.Empty;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var firstBatchEdges = new List<RelayEdge<INode>>();
            for (int i = 0; i < 8; i++)
            {
                var node = MockNode.Create($"node{i}");
                var hasTypeCursor = new HasTypeCursor($"node{i}", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
                var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
                var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));
                firstBatchEdges.Add(new RelayEdge<INode>(cursor, node));
            }

            var secondBatchEdges = new List<RelayEdge<INode>>();
            for (int i = 8; i < 15; i++)
            {
                var node = MockNode.Create($"node{i}");
                var hasTypeCursor = new HasTypeCursor($"node{i}", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
                var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
                var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));
                secondBatchEdges.Add(new RelayEdge<INode>(cursor, node));
            }

            var callCount = 0;
            var firstConnection = new Connection<RelayEdge<INode>, INode>(
                firstBatchEdges.ToImmutableList(),
                new PageInfo(true, false, firstBatchEdges[0].Cursor, firstBatchEdges[^1].Cursor));

            var secondConnection = new Connection<RelayEdge<INode>, INode>(
                secondBatchEdges.ToImmutableList(),
                new PageInfo(false, false, secondBatchEdges[0].Cursor, secondBatchEdges[^1].Cursor));

            graphQueryService.SetConnectionCallback(() =>
            {
                callCount++;
                return callCount == 1 ? firstConnection : secondConnection;
            });

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, null, null, null), 25, true, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<NodeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(10, result.Connection.Edges.Count);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithMultiValuePropertyFilter()
        {
            var cancellationToken = GetCancellationToken();
            var key = string.Empty;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var propertyValues = ImmutableList<string>.Empty
                .Add("value1")
                .Add("value2");

            var filterData = new NodeFilterArguments(PropertyOperator.Equals, propertyValues);
            var orderData = new OrderArguments("name", OrderDirection.Asc);
            var pushdownData = new NodePushdownQueryData(orderData, filterData);

            nodeFilterService.SetPushdownQueryData(pushdownData);

            var connection = Connection<RelayEdge<INode>, INode>.Empty;
            graphQueryService.SetConnectionByTypePropertyNameAndValuesResponse(connection);

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, null, null, null), 25, true, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.IsTrue(graphQueryService.GetConnectionByTypePropertyNameAndValuesAsyncCalled);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithMultiValuePropertyFilterDescending()
        {
            var cancellationToken = GetCancellationToken();
            var key = string.Empty;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var propertyValues = ImmutableList<string>.Empty
                .Add("value1")
                .Add("value2");

            var filterData = new NodeFilterArguments(PropertyOperator.Equals, propertyValues);
            var orderData = new OrderArguments("name", OrderDirection.Desc);
            var pushdownData = new NodePushdownQueryData(orderData, filterData);

            nodeFilterService.SetPushdownQueryData(pushdownData);

            var connection = Connection<RelayEdge<INode>, INode>.Empty;
            graphQueryService.SetConnectionByTypePropertyNameAndValuesResponse(connection);

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, null, null, null), 25, true, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.IsTrue(graphQueryService.GetConnectionByTypePropertyNameAndValuesAsyncCalled);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithSingleValuePropertyFilter()
        {
            var cancellationToken = GetCancellationToken();
            var key = string.Empty;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var propertyValues = ImmutableList<string>.Empty
                .Add("value1");

            var filterData = new NodeFilterArguments(PropertyOperator.Equals, propertyValues);
            var orderData = new OrderArguments("name", OrderDirection.Asc);
            var pushdownData = new NodePushdownQueryData(orderData, filterData);

            nodeFilterService.SetPushdownQueryData(pushdownData);

            var connection = Connection<RelayEdge<INode>, INode>.Empty;
            graphQueryService.SetConnectionByTypePropertyNameAndValueResponse(connection);

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, null, null, null), 25, true, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.IsTrue(graphQueryService.GetConnectionByTypePropertyNameAndValueAsyncCalled);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithOrderedQueryNoFilter()
        {
            var cancellationToken = GetCancellationToken();
            var key = string.Empty;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var nodeFilterService = new MockNodeFilterService();

            var orderData = new OrderArguments("name", OrderDirection.Asc);
            var pushdownData = new NodePushdownQueryData(orderData, null);

            nodeFilterService.SetPushdownQueryData(pushdownData);

            var connection = Connection<RelayEdge<INode>, INode>.Empty;
            graphQueryService.SetConnectionByTypeAndPropertyNameResponse(connection);

            var executor = new NodeConnectionQueryExecutor(
                graphQueryService,
                nodeFilterService,
                cursorSerializer);

            var query = new NodeConnectionQuery("User", null, null, new ConnectionArguments(10, null, null, null), 25, true, null);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty);

            var resultContext = await executor.ExecuteAsync(context, key, cancellationToken);

            Assert.IsNotNull(resultContext);
            Assert.IsTrue(graphQueryService.GetConnectionByTypeAndPropertyNameAsyncCalled);
        }
    }
}
