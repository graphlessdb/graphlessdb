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
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class ToEdgeConnectionQueryExecutorTests
    {
        private static CancellationToken GetCancellationToken()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
        }

        [TestMethod]
        public async Task CanExecuteAsync()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, 100, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor, cursor));
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => "inId",
                cn => "outId",
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }


        [TestMethod]
        public async Task ExecuteAsyncWithEndOfDataCursorReturnsEmptyConnection()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var childKey = "childKey";
            var edgeTypeName = "TestEdge";

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var cursorNode = CursorNode.CreateEndOfData();
            var cursor = cursorSerializer.Serialize(new Cursor(ImmutableTree<string, CursorNode>.Empty.AddNode("root", cursorNode)));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, new ConnectionArguments(10, cursor, null, null), 100, false, null);
            var node = MockNode.Create("node1");
            var relayNode = new RelayEdge<INode>("nodeCursor", node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, "nodeCursor", "nodeCursor"));
            var nodeResult = new NodeConnectionResult(null, "nodeCursor", false, false, nodeConnection);

            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => "inId",
                cn => "outId",
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Connection.Edges.Count);
            Assert.IsFalse(result.NeedsMoreData);
            Assert.IsFalse(result.HasMoreData);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithPostFilteringUsesPreFilteredPageSize()
        {
            var cancellationToken = GetCancellationToken();
            var key = "testKey";
            var nodeId = "node1";
            var edgeTypeName = "TestEdge";
            var preFilteredPageSize = 200;

            var cursorSerializer = new GraphCursorSerializationService();
            var graphQueryService = new MockGraphQueryService();
            var edgeFilterService = new MockEdgeFilterService();
            edgeFilterService.SetIsPostFilteringRequired(true);

            var executor = new ToEdgeConnectionQueryExecutor(
                cursorSerializer,
                graphQueryService,
                edgeFilterService);

            var hasTypeCursor = new HasTypeCursor(nodeId, "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = cursorSerializer.Serialize(Cursor.Create(cursorNode));

            var query = new MockToEdgeConnectionQuery(edgeTypeName, null, null, ConnectionArguments.Default, preFilteredPageSize, false, null);
            var node = MockNode.Create(nodeId);
            var relayNode = new RelayEdge<INode>(cursor, node);
            var nodeConnection = new Connection<RelayEdge<INode>, INode>([relayNode], new PageInfo(false, false, cursor, cursor));
            var nodeResult = new NodeConnectionResult(null, cursor, false, false, nodeConnection);

            var childKey = "childKey";
            var graphQuery = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(childKey, new GraphQueryNode(new WhereNodeConnectionQuery(_ => Task.FromResult(true), ConnectionArguments.Default, 100, false, null)))
                .AddParentNode(childKey, key, new GraphQueryNode(query));

            var context = new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                graphQuery,
                ImmutableDictionary<string, GraphResult>.Empty
                    .Add(childKey, nodeResult));

            var resultContext = await executor.ExecuteAsync(
                context,
                key,
                q => "TestNodeType",
                cn => "inId",
                cn => "outId",
                (req, ct) => Task.FromResult(new ToEdgeQueryResponse(Connection<RelayEdge<EdgeKey>, EdgeKey>.Empty)),
                cancellationToken);

            Assert.IsNotNull(resultContext);
            var result = resultContext.TryGetResult<EdgeConnectionResult>(key);
            Assert.IsNotNull(result);
        }

        private sealed record MockToEdgeConnectionQuery(
            string? EdgeTypeName,
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

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            public Task ClearAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(TryGetVersionedNodesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetEdgesResponse> TryGetEdgesAsync(TryGetEdgesRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(new TryGetEdgesResponse(ImmutableList<RelayEdge<IEdge>?>.Empty));
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAsync(GetConnectionByTypeRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(GetConnectionByTypeAndPropertyNameRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(GetConnectionByTypePropertyNameAndValueRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(GetConnectionByTypePropertyNameAndValuesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
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

        private sealed class MockEdgeFilterService : IGraphEdgeFilterService
        {
            private bool isPostFilteringRequired;

            public void SetIsPostFilteringRequired(bool value)
            {
                isPostFilteringRequired = value;
            }

            public bool IsPostFilteringRequired(IEdgeFilter? filter)
            {
                return isPostFilteringRequired;
            }

            public EdgePushdownQueryData? TryGetEdgePushdownQueryData(string? edgeTypeName, IEdgeFilter? filter, IEdgeOrder? order)
            {
                return null;
            }

            public Task<bool> IsFilterMatchAsync(IEdge edge, IEdgeFilter? filter, bool consistentRead, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }
    }
}
