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
    public sealed class FluentGraphQueryTests
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
        public async Task NodeWithIdAndTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user.Id, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeWithIdAndNoTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeGenericWithIdAndTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user.Id, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeGenericWithIdAndNoTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeWithNodeInstanceAndTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeWithNodeInstanceAndNoTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeOrDefaultWithIdAndTagReturnsFluentNodeOrDefaultQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeOrDefault(user.Id, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeOrDefaultWithIdAndNoTagReturnsFluentNodeOrDefaultQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeOrDefault(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeOrDefaultGenericWithIdAndTagReturnsFluentNodeOrDefaultQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeOrDefault<User>(user.Id, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeOrDefaultGenericWithIdAndNoTagReturnsFluentNodeOrDefaultQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeOrDefault<User>(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeVersionWithNodeInstanceAndVersionReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion(user, 0)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeVersionWithNodeInstanceVersionAndTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion(user, 0, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeVersionGenericWithIdAndVersionReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion<User>(user.Id, 0)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeVersionGenericWithIdVersionAndTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion<User>(user.Id, 0, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeVersionNonGenericWithIdAndVersionReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion(user.Id, 0)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodeVersionNonGenericWithIdVersionAndTagReturnsFluentNodeQuery()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion(user.Id, 0, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task NodesWithConfigureReturnsFluentNodeConnectionQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(opts => opts with { PageSize = 10 })
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 2);
        }

        [TestMethod]
        public async Task NodesWithNullConfigureReturnsFluentNodeConnectionQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User>(null)
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 1);
        }

        [TestMethod]
        public async Task NodesWithTypedFilterAndOrderReturnsFluentNodeConnectionQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User, UserFilter, UserOrder>(opts => opts with { Order = new UserOrder { Username = OrderDirection.Asc } })
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 2);
        }

        [TestMethod]
        public async Task NodesWithTypedFilterAndOrderNullConfigureReturnsFluentNodeConnectionQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Nodes<User, UserFilter, UserOrder>(null)
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Edges.Count >= 1);
        }

        [TestMethod]
        public async Task EdgeWithEdgeInstanceReturnsFluentEdgeQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(edge)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeWithEdgeInstanceAndTagReturnsFluentEdgeQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(edge, "myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeWithInIdAndOutIdReturnsFluentEdgeQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(user1.Id, user2.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeWithInIdOutIdAndConfigureReturnsFluentEdgeQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(user1.Id, user2.Id, opts => opts with { Tag = "myTag" })
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeWithInIdOutIdAndNullConfigureReturnsFluentEdgeQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(user1.Id, user2.Id, null)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeOrDefaultWithInIdAndOutIdReturnsFluentEdgeOrDefaultQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .EdgeOrDefault<UserLikesUserEdge, User, User>(user1.Id, user2.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeOrDefaultWithInIdOutIdAndConfigureReturnsFluentEdgeOrDefaultQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .EdgeOrDefault<UserLikesUserEdge, User, User>(user1.Id, user2.Id, opts => opts with { Tag = "myTag" })
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task EdgeOrDefaultWithInIdOutIdAndNullConfigureReturnsFluentEdgeOrDefaultQuery()
        {
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

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .EdgeOrDefault<UserLikesUserEdge, User, User>(user1.Id, user2.Id, null)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task PutWithPutQueryReturnsFluentPut()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");
            var putQuery = new PutQuery([user], [], [], [], false);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(putQuery)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task PutWithSingleEntityReturnsFluentPut()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user.Id, result.Id);
        }

        [TestMethod]
        public async Task PutWithEntityArrayReturnsFluentPut()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2)
                .ExecuteAsync(cancellationToken);

            var result1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user1.Id)
                .GetAsync(true, cancellationToken);

            var result2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user2.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result1);
            Assert.IsNotNull(result2);
            Assert.AreEqual(user1.Id, result1.Id);
            Assert.AreEqual(user2.Id, result2.Id);
        }

        [TestMethod]
        public async Task PutWithImmutableListReturnsFluentPut()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var entities = ImmutableList.Create(user1, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(entities)
                .ExecuteAsync(cancellationToken);

            var result1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user1.Id)
                .GetAsync(true, cancellationToken);

            var result2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node<User>(user2.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result1);
            Assert.IsNotNull(result2);
            Assert.AreEqual(user1.Id, result1.Id);
            Assert.AreEqual(user2.Id, result2.Id);
        }

        [TestMethod]
        public async Task ClearReturnsFluentClear()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user = User.New("testuser");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeOrDefault<User>(user.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNull(result);
        }
    }
}
