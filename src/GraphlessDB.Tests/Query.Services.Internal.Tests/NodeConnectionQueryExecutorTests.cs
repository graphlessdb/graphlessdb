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

            public void SetNode(string id, INode node)
            {
                nodes[id] = node;
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
                return Task.FromResult(new GetConnectionResponse(Connection<RelayEdge<INode>, INode>.Empty));
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(GetConnectionByTypeAndPropertyNameRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(new GetConnectionResponse(Connection<RelayEdge<INode>, INode>.Empty));
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(GetConnectionByTypePropertyNameAndValueRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(new GetConnectionResponse(Connection<RelayEdge<INode>, INode>.Empty));
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(GetConnectionByTypePropertyNameAndValuesRequest request, CancellationToken cancellationToken)
            {
                return Task.FromResult(new GetConnectionResponse(Connection<RelayEdge<INode>, INode>.Empty));
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
            public bool IsPostFilteringRequired(INodeFilter? filter)
            {
                return false;
            }

            public NodePushdownQueryData? TryGetNodePushdownQueryData(
                string typeName,
                INodeFilter? filter,
                INodeOrder? order,
                CancellationToken cancellationToken)
            {
                return null;
            }

            public Task<bool> IsFilterMatchAsync(
                INode node,
                INodeFilter? filter,
                bool consistentRead,
                CancellationToken cancellationToken)
            {
                return Task.FromResult(true);
            }
        }
    }
}
