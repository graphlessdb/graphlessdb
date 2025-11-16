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
    public sealed class FluentEdgeQueryTests
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
        public async Task GetAsyncWithUseConsistentReadReturnsEdge()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
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
                .Edge<UserLikesUserEdge, User, User>(edge)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GetAsyncWithoutUseConsistentReadReturnsEdge()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
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
                .Edge<UserLikesUserEdge, User, User>(edge)
                .GetAsync(false, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GetAsyncWithConnectionSizesReturnsEdge()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
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
                .Edge<UserLikesUserEdge, User, User>(edge)
                .GetAsync(true, 10, 10, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GetAsyncWithConfigureReturnsEdge()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
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
                .Edge<UserLikesUserEdge, User, User>(edge)
                .GetAsync(c => c.WithConsistentRead(true), cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task InFromEdgeReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get node from edge
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(edge)
                .InFromEdge()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user1.Id, result.Id);
        }

        [TestMethod]
        public async Task InFromEdgeWithTagReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get node from edge with tag
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(edge)
                .InFromEdge("myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user1.Id, result.Id);
        }

        [TestMethod]
        public async Task OutFromEdgeReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get node from edge
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(edge)
                .OutFromEdge()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user2.Id, result.Id);
        }

        [TestMethod]
        public async Task OutFromEdgeWithTagReturnsFluentNodeQuery()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge = UserLikesUserEdge.New(user1, user2);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user1, user2, edge))
                .ExecuteAsync(cancellationToken);

            // Get node from edge with tag
            var result = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(edge)
                .OutFromEdge("myTag")
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(user2.Id, result.Id);
        }
    }
}
