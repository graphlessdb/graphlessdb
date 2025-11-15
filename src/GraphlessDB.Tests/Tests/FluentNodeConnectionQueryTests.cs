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
    public sealed class FluentNodeConnectionQueryTests
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
        public async Task GetEntitiesAsyncReturnsImmutableListOfNodes()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var nodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .GetEntitiesAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(nodes);
            Assert.IsTrue(nodes.Count >= 2);
        }

        [TestMethod]
        public async Task GetAsyncWithUseConsistentReadAndConnectionArgumentsReturnsConnection()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(connection);
            Assert.IsTrue(connection.Edges.Count >= 2);
        }

        [TestMethod]
        public async Task GetAsyncWithConnectionSizesReturnsConnection()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .GetAsync(true, ConnectionArguments.FirstMax, 10, 10, cancellationToken);

            Assert.IsNotNull(connection);
            Assert.IsTrue(connection.Edges.Count >= 2);
        }

        [TestMethod]
        public async Task GetAsyncWithConfigureFuncReturnsConnection()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .GetAsync(c => c.WithConsistentRead(true).WithConnectionArguments(ConnectionArguments.FirstMax), cancellationToken);

            Assert.IsNotNull(connection);
            Assert.IsTrue(connection.Edges.Count >= 1);
        }

        [TestMethod]
        public async Task FirstReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .First()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
        }

        [TestMethod]
        public async Task FirstWithTagReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .First("myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
        }

        [TestMethod]
        public async Task SingleReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Clear and add only one user
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            var user1 = User.New("user1");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
            Assert.AreEqual(user1.Id, node.Id);
        }

        [TestMethod]
        public async Task SingleWithTagReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Clear and add only one user
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            var user1 = User.New("user1");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .Single("myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
            Assert.AreEqual(user1.Id, node.Id);
        }

        [TestMethod]
        public async Task FirstOrDefaultReturnsFluentNodeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .FirstOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
        }

        [TestMethod]
        public async Task FirstOrDefaultWithTagReturnsFluentNodeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .FirstOrDefault("myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
        }

        [TestMethod]
        public async Task SingleOrDefaultReturnsFluentNodeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Clear and add only one user
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            var user1 = User.New("user1");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .SingleOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
            Assert.AreEqual(user1.Id, node.Id);
        }

        [TestMethod]
        public async Task SingleOrDefaultWithTagReturnsFluentNodeOrDefaultQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Clear and add only one user
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            var user1 = User.New("user1");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .SingleOrDefault("myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);
            Assert.AreEqual(user1.Id, node.Id);
        }

        [TestMethod]
        public async Task InToEdgesReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edge1 = UserLikesUserEdge.New(user1, user2);
            var edge2 = UserLikesUserEdge.New(user3, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, user3, edge1, edge2))
                .ExecuteAsync(cancellationToken);

            // Get edges where users in the connection are IN nodes
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user1" } } })
                .InToEdges<UserLikesUserEdge, User>()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task InToEdgesWithConfigureReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user1" } } })
                .InToEdges<UserLikesUserEdge, User>(opts => opts with { PageSize = 10 })
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task InToEdgesWithNullConfigureReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user1" } } })
                .InToEdges<UserLikesUserEdge, User>(null)
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task OutToEdgesReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            // OutToEdges finds edges where the nodes in the connection are OUT nodes
            var edge1 = UserLikesUserEdge.New(user1, user2);
            var edge2 = UserLikesUserEdge.New(user3, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, user3, edge1, edge2))
                .ExecuteAsync(cancellationToken);

            // Get edges where user2 is OUT node
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } })
                .OutToEdges<UserLikesUserEdge, User>()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
        }

        [TestMethod]
        public async Task OutToEdgesWithConfigureReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } })
                .OutToEdges<UserLikesUserEdge, User>(opts => opts with { PageSize = 10 })
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task OutToEdgesWithNullConfigureReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } })
                .OutToEdges<UserLikesUserEdge, User>(null)
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task InAndOutToEdgesReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edge1 = UserLikesUserEdge.New(user2, user1);
            var edge2 = UserLikesUserEdge.New(user2, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, user3, edge1, edge2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } })
                .InAndOutToEdges<UserLikesUserEdge>()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 1);
        }

        [TestMethod]
        public async Task InAndOutToEdgesWithConfigureReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user2, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } })
                .InAndOutToEdges<UserLikesUserEdge>(opts => opts with { PageSize = 10 })
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task InAndOutToEdgesWithNullConfigureReturnsFluentEdgeConnectionQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user2, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } })
                .InAndOutToEdges<UserLikesUserEdge>(null)
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
        }

        [TestMethod]
        public async Task ZipCombinesTwoNodeConnectionQueries()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var query1 = services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user1" } } });

            var query2 = services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } });

            var result = await query1
                .Zip(query2)
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 1);
        }

        [TestMethod]
        public async Task ZipWithTagCombinesTwoNodeConnectionQueries()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var query1 = services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user1" } } });

            var query2 = services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { Filter = new UserFilter { Username = new StringFilter { Eq = "user2" } } });

            var result = await query1
                .Zip(query2, "myTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 1);
        }

        [TestMethod]
        public async Task WhereAsyncFiltersNodesWithRelayNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .WhereAsync(ctx => Task.FromResult(ctx.Item.Node.Username == "user1"))
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereAsyncWithTagFiltersNodesWithRelayNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .WhereAsync(ctx => Task.FromResult(ctx.Item.Node.Username == "user1"), "myTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereNodeAsyncFiltersNodesWithNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .WhereNodeAsync(ctx => Task.FromResult(ctx.Item.Username == "user2"))
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user2", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereNodeAsyncWithTagFiltersNodesWithNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .WhereNodeAsync(ctx => Task.FromResult(ctx.Item.Username == "user2"), "myTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user2", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereFiltersNodesWithRelayNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .Where(ctx => ctx.Item.Node.Username == "user1")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereWithTagFiltersNodesWithRelayNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .Where(ctx => ctx.Item.Node.Username == "user1", "myTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereNodeFiltersNodesWithNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .WhereNode(ctx => ctx.Item.Username == "user2")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user2", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task WhereNodeWithTagFiltersNodesWithNodeContext()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2))
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .WhereNode(ctx => ctx.Item.Username == "user2", "myTag")
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user2", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task AnyAsyncReturnsTrueWhenNodesExist()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1)
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .AnyAsync(true, cancellationToken);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task AnyAsyncReturnsFalseWhenNoNodesExist()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Clear all users
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            // Get
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>()
                .AnyAsync(true, cancellationToken);

            Assert.IsFalse(result);
        }
    }
}
