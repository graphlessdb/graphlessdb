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
using GraphlessDB.Extensions.DependencyInjection;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.Internal;
using GraphlessDB.Tests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class RDFTripleGraphQueryServiceTests
    {
        private static ServiceProvider CreateServiceProvider()
        {
            var services = new ServiceCollection();
            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "TestGraph";
                    o.PartitionCount = 2;
                })
                .AddGraphEntityTypeNativeServiceOptions(o =>
                {
                    o.AddTestGraphEntityTypeMappings();
                })
                .AddGraphlessDBEntitySerializerOptions(o =>
                {
                    o.JsonContext = GraphlessDBTestContext.Default;
                });

            services
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, TestGraphGraphQueryablePropertyService>();

            return services.BuildServiceProvider();
        }

        [TestMethod]
        public async Task TryGetNodesAsyncReturnsEmptyWhenIdsIsEmpty()
        {
            var services = CreateServiceProvider();
            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new TryGetNodesRequest(ImmutableList<string>.Empty, false);
            var response = await queryService.TryGetNodesAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.AreEqual(0, response.Nodes.Count);
        }

        [TestMethod]
        public async Task TryGetNodesAsyncReturnsNodesWhenIdsProvided()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user = User.New("testuser");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new TryGetNodesRequest([user.Id], false);
            var response = await queryService.TryGetNodesAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.AreEqual(1, response.Nodes.Count);
            Assert.IsNotNull(response.Nodes[0]);
        }

        [TestMethod]
        public async Task TryGetEdgesAsyncReturnsEmptyWhenKeysIsEmpty()
        {
            var services = CreateServiceProvider();
            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new TryGetEdgesRequest(ImmutableList<EdgeKey>.Empty, false);
            var response = await queryService.TryGetEdgesAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.AreEqual(0, response.Edges.Count);
        }

        [TestMethod]
        public async Task TryGetVersionedNodesAsyncReturnsEmptyWhenKeysIsEmpty()
        {
            var services = CreateServiceProvider();
            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new TryGetVersionedNodesRequest(ImmutableList<VersionedNodeKey>.Empty, false);
            var response = await queryService.TryGetVersionedNodesAsync(request, CancellationToken.None);

            Assert.IsNotNull(response);
            Assert.AreEqual(0, response.Nodes.Count);
        }

        [TestMethod]
        public async Task ClearAsyncDeletesAllPartitions()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user = User.New("testuser");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            await queryService.ClearAsync(cancellationToken);

            var cantGetPerson = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .UserOrDefault(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNull(cantGetPerson);
        }

        [TestMethod]
        public async Task GetConnectionByTypeAsyncReturnsConnectionWithEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new GetConnectionByTypeRequest("User", ConnectionArguments.GetFirst(10), false);
            var response = await queryService.GetConnectionByTypeAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
            Assert.IsTrue(response.Connection.Edges.Count >= 2);
        }

        [TestMethod]
        public async Task GetInToEdgeConnectionAsyncReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user2.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user2)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                null,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetInToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetOutToEdgeConnectionAsyncReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                null,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetInAndOutToEdgeConnectionAsyncReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                null,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetInAndOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public async Task GetInAndOutToEdgeConnectionAsyncThrowsWhenEdgeTypeNameIsNull()
        {
            var services = CreateServiceProvider();
            var user1 = User.New("user1");
            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>("cursor1", user1)],
                new PageInfo(false, false, "cursor1", "cursor1"));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new ToEdgeQueryRequest(
                "User",
                null,
                nodeConnection,
                null,
                null,
                ConnectionArguments.GetFirst(10),
                false);

            await queryService.GetInAndOutToEdgeConnectionAsync(request, CancellationToken.None);
        }

        [TestMethod]
        public async Task GetConnectionByTypeAndPropertyNameAsyncReturnsConnection()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user = User.New("testuser");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new GetConnectionByTypeAndPropertyNameRequest(
                "User",
                "Username",
                false,
                ConnectionArguments.GetFirst(10),
                false);
            var response = await queryService.GetConnectionByTypeAndPropertyNameAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetConnectionByTypePropertyNameAndValueAsyncReturnsConnection()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user = User.New("testuser");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new GetConnectionByTypePropertyNameAndValueRequest(
                "User",
                "Username",
                PropertyOperator.Equals,
                "testuser",
                false,
                ConnectionArguments.GetFirst(10),
                false);
            var response = await queryService.GetConnectionByTypePropertyNameAndValueAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetConnectionByTypePropertyNameAndValuesAsyncReturnsConnection()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new GetConnectionByTypePropertyNameAndValuesRequest(
                "User",
                "Username",
                PropertyOperator.Equals,
                ["user1", "user2"],
                false,
                ConnectionArguments.GetFirst(10),
                false);
            var response = await queryService.GetConnectionByTypePropertyNameAndValuesAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task PutAsyncAddsNewNode()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user = User.New("testuser");
            var queryService = services.GetRequiredService<IGraphQueryService>();

            var putRequest = new PutRequest(
                new MutationId("mut1"),
                [user],
                ImmutableList<INode>.Empty,
                ImmutableList<EdgeByPropCheck>.Empty,
                ImmutableList<string>.Empty,
                false);

            await queryService.PutAsync(putRequest, cancellationToken);

            var getRequest = new TryGetNodesRequest([user.Id], false);
            var getResponse = await queryService.TryGetNodesAsync(getRequest, cancellationToken);

            Assert.AreEqual(1, getResponse.Nodes.Count);
            Assert.IsNotNull(getResponse.Nodes[0]);
        }

        [TestMethod]
        public async Task TryGetVersionedNodesAsyncReturnsVersionedNodes()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user = User.New("testuser");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new TryGetVersionedNodesRequest(
                [new VersionedNodeKey(user.Id, 1)],
                false);
            var response = await queryService.TryGetVersionedNodesAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.AreEqual(1, response.Nodes.Count);
        }

        [TestMethod]
        public async Task TryGetEdgesAsyncReturnsEdgesWhenKeysProvided()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new TryGetEdgesRequest(
                [new EdgeKey("UserLikesUserEdge", user2.Id, user1.Id)],
                false);
            var response = await queryService.TryGetEdgesAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.AreEqual(1, response.Edges.Count);
        }

        [TestMethod]
        public async Task GetInAndOutToEdgeConnectionWithPaginationReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edge1 = UserLikesUserEdge.New(user1, user2);
            var edge2 = UserLikesUserEdge.New(user1, user3);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, user3, edge1, edge2)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                null,
                ConnectionArguments.GetFirst(1),
                false);

            var response = await queryService.GetInAndOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetInAndOutToEdgeConnectionWithFilterByReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var filterBy = new EdgeFilterArguments("LikesUsername", PropertyOperator.Equals, "user2");
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                filterBy,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetInAndOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetInAndOutToEdgeConnectionWithOrderByReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var orderBy = new OrderArguments("LikesUsername", OrderDirection.Asc);
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                orderBy,
                null,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetInAndOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetOutToEdgeConnectionWithFilterByReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var filterBy = new EdgeFilterArguments("LikesUsername", PropertyOperator.Equals, "user2");
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                filterBy,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetOutToEdgeConnectionWithOrderByReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user1.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user1)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var orderBy = new OrderArguments("LikesUsername", OrderDirection.Asc);
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                orderBy,
                null,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetOutToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }

        [TestMethod]
        public async Task GetInToEdgeConnectionWithFilterByReturnsEdges()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = CreateServiceProvider();

            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, edge)
                .ExecuteAsync(cancellationToken);

            var cursorSerializer = new GraphCursorSerializationService();
            var hasTypeCursor = new HasTypeCursor(user2.Id, "0", []);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                [new RelayEdge<INode>(cursorText, user2)],
                new PageInfo(false, false, cursorText, cursorText));

            var queryService = services.GetRequiredService<IGraphQueryService>();
            var filterBy = new EdgeFilterArguments("LikedByUsername", PropertyOperator.Equals, "user1");
            var request = new ToEdgeQueryRequest(
                "User",
                "UserLikesUserEdge",
                nodeConnection,
                null,
                filterBy,
                ConnectionArguments.GetFirst(10),
                false);

            var response = await queryService.GetInToEdgeConnectionAsync(request, cancellationToken);

            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Connection);
        }
    }
}
