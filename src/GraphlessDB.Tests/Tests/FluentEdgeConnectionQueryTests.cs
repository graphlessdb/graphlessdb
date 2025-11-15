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
using GraphlessDB;
using GraphlessDB.Extensions.DependencyInjection;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Graph.Services.Internal.Tests;
using GraphlessDB.Query.Services;
using GraphlessDB.Query.Services.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class FluentEdgeConnectionQueryTests
    {
        private static ServiceProvider GetServiceProvider()
        {
            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                })
                .AddGraphEntityTypeNativeServiceOptions(o =>
                {
                    o.TypeMappings.Add(nameof(Car), typeof(Car));
                    o.TypeMappings.Add(nameof(Manufacturer), typeof(Manufacturer));
                    o.TypeMappings.Add(nameof(ManufacturerMakesCarEdge), typeof(ManufacturerMakesCarEdge));
                    o.TypeMappings.Add(nameof(User), typeof(User));
                    o.TypeMappings.Add(nameof(UserLikesUserEdge), typeof(UserLikesUserEdge));
                    o.TypeMappings.Add(nameof(UserOwnsCarEdge), typeof(UserOwnsCarEdge));
                })
                .AddGraphlessDBEntitySerializerOptions(o =>
                {
                    o.JsonContext = GraphlessDBTestContext.Default;
                });

            services
                .AddTestInstrumentation(Debugger.IsAttached)
                .AddGraphlessDBWithInMemoryDB()
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, TestGraphGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>();

            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
        }

        [TestMethod]
        public async Task GetEntitiesAsyncReturnsEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .GetEntitiesAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(2, edges.Count);
        }

        [TestMethod]
        public async Task GetAsyncWithUseConsistentReadAndConnectionArgumentsReturnsConnection()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(connection);
            Assert.AreEqual(2, connection.Edges.Count);
        }

        [TestMethod]
        public async Task AnyAsyncReturnsTrueWhenEdgesExist()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var hasAny = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .AnyAsync(true, cancellationToken);

            Assert.IsTrue(hasAny);
        }

        [TestMethod]
        public async Task AnyAsyncReturnsFalseWhenNoEdgesExist()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user0)
                .ExecuteAsync(cancellationToken);

            var hasAny = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .AnyAsync(true, cancellationToken);

            Assert.IsFalse(hasAny);
        }

        [TestMethod]
        public async Task GetAsyncWithConfigureFunctionReturnsConnection()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .GetAsync(c => c.WithConsistentRead(true).WithConnectionArguments(ConnectionArguments.FirstMax), cancellationToken);

            Assert.IsNotNull(connection);
            Assert.AreEqual(1, connection.Edges.Count);
        }

        [TestMethod]
        public async Task SingleReturnsFluentEdgeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge.OutId);
        }

        [TestMethod]
        public async Task SingleWithTagReturnsFluentEdgeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .Single("testTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge.OutId);
        }

        [TestMethod]
        public async Task SingleOrDefaultReturnsFluentEdgeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .SingleOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge!.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge!.OutId);
        }

        [TestMethod]
        public async Task SingleOrDefaultWithTagReturnsFluentEdgeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .SingleOrDefault("testTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge!.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge!.OutId);
        }

        [TestMethod]
        public async Task FirstReturnsFluentEdgeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .First()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
        }

        [TestMethod]
        public async Task FirstWithTagReturnsFluentEdgeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .First("testTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge.OutId);
        }

        [TestMethod]
        public async Task FirstOrDefaultReturnsFluentEdgeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .FirstOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge!.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge!.OutId);
        }

        [TestMethod]
        public async Task FirstOrDefaultWithTagReturnsFluentEdgeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .FirstOrDefault("testTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual(user0LikesUser1.InId, edge!.InId);
            Assert.AreEqual(user0LikesUser1.OutId, edge!.OutId);
        }

        [TestMethod]
        public async Task InFromEdgesReturnsNodeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .InFromEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.AreEqual(1, nodes.Edges.Count);
            Assert.AreEqual(user0.Id, nodes.Edges[0].Node.Id);
        }

        [TestMethod]
        public async Task InFromEdgesWithTagReturnsNodeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .InFromEdges("testTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.AreEqual(1, nodes.Edges.Count);
        }

        [TestMethod]
        public async Task OutFromEdgesReturnsNodeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .OutFromEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.AreEqual(1, nodes.Edges.Count);
            Assert.AreEqual(user1.Id, nodes.Edges[0].Node.Id);
        }

        [TestMethod]
        public async Task OutFromEdgesWithTagReturnsNodeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .OutFromEdges("testTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.AreEqual(1, nodes.Edges.Count);
        }

        [TestMethod]
        public async Task InAndOutFromEdgesReturnsNodeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .InAndOutFromEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.AreEqual(2, nodes.Edges.Count);
        }

        [TestMethod]
        public async Task InAndOutFromEdgesWithConfigureReturnsNodeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0LikesUser1))
                .ExecuteAsync(cancellationToken);

            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .InAndOutFromEdges(opts => opts with { Tag = "testTag" })
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.AreEqual(2, nodes.Edges.Count);
        }

        [TestMethod]
        public async Task WhereAsyncFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .WhereAsync(ctx => Task.FromResult(ctx.Item.Node.LikesUsername == "User1"))
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
            Assert.AreEqual(user1.Username, edges.Edges[0].Node.LikesUsername);
        }

        [TestMethod]
        public async Task WhereAsyncWithTagFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .WhereAsync(ctx => Task.FromResult(ctx.Item.Node.LikesUsername == "User1"), "testTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
        }

        [TestMethod]
        public async Task WhereEdgeAsyncFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .WhereEdgeAsync(ctx => Task.FromResult(ctx.Item.LikesUsername == "User1"))
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
            Assert.AreEqual("User1", edges.Edges[0].Node.LikesUsername);
        }

        [TestMethod]
        public async Task WhereEdgeAsyncWithTagFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .WhereEdgeAsync(ctx => Task.FromResult(ctx.Item.LikesUsername == "User1"), "testTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
        }

        [TestMethod]
        public async Task WhereFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .Where(ctx => ctx.Item.Node.LikesUsername == "User1")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
            Assert.AreEqual(user1.Username, edges.Edges[0].Node.LikesUsername);
        }

        [TestMethod]
        public async Task WhereWithTagFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .Where(ctx => ctx.Item.Node.LikesUsername == "User1", "testTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
        }

        [TestMethod]
        public async Task WhereEdgeFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .WhereEdge(ctx => ctx.Item.LikesUsername == "User1")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
            Assert.AreEqual("User1", edges.Edges[0].Node.LikesUsername);
        }

        [TestMethod]
        public async Task WhereEdgeWithTagFiltersEdges()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0LikesUser1 = UserLikesUserEdge.New(user0, user1);
            var user0LikesUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0LikesUser1, user0LikesUser2))
                .ExecuteAsync(cancellationToken);

            var edges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(user0.Id)
                .InToEdges<UserLikesUserEdge, User, UserLikesUserEdgeFilter, UserLikesUserEdgeOrder>()
                .WhereEdge(ctx => ctx.Item.LikesUsername == "User1", "testTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(edges);
            Assert.AreEqual(1, edges.Edges.Count);
        }
    }
}
