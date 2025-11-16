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
using GraphlessDB.Collections;
using GraphlessDB.Extensions.DependencyInjection;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Graph.Services.Internal.Tests;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;
using GraphlessDB.Query.Services.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class FluentEdgeFromRelayEdgeQueryTests
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

        private static FluentEdgeFromRelayEdgeQuery CreateNonGenericQuery(IGraphQueryExecutionService service, string key, string typeName, string inId, string outId)
        {
            var query = ImmutableTree<string, GraphQueryNode>.Empty
                .AddNode(key, new GraphQueryNode(new EdgeOrDefaultByIdQuery(typeName, inId, outId, false, null)));

            return new FluentEdgeFromRelayEdgeQuery(
                service,
                query,
                key);
        }

        private static FluentEdgeFromRelayEdgeQuery<UserLikesUserEdge> CreateGenericQuery(IGraphQueryExecutionService service, string key, string typeName, string inId, string outId)
        {
            var query = ImmutableTree<string, GraphQueryNode>.Empty
                .AddNode(key, new GraphQueryNode(new EdgeOrDefaultByIdQuery(typeName, inId, outId, false, null)));

            return new FluentEdgeFromRelayEdgeQuery<UserLikesUserEdge>(
                service,
                query,
                key);
        }

        [TestMethod]
        public async Task GetAsyncReturnsEdge()
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
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var query = CreateNonGenericQuery(graphQueryService, "root", nameof(UserLikesUserEdge), edge.InId, edge.OutId);

            var result = await query.GetAsync(q => q, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GetAsyncWithConfigureFuncReturnsEdge()
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
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var query = CreateNonGenericQuery(graphQueryService, "root", nameof(UserLikesUserEdge), edge.InId, edge.OutId);

            var result = await query.GetAsync(q => q.WithConsistentRead(true), cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GetAsyncThrowsWhenEdgeNotFound()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Get
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var nonExistentInId = GlobalId.Get<User>(Guid.NewGuid().ToString());
            var nonExistentOutId = GlobalId.Get<User>(Guid.NewGuid().ToString());
            var query = CreateNonGenericQuery(graphQueryService, "root", nameof(UserLikesUserEdge), nonExistentInId, nonExistentOutId);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(
                async () => await query.GetAsync(q => q, cancellationToken));
        }

        [TestMethod]
        public async Task GenericGetAsyncReturnsEdge()
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
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var query = CreateGenericQuery(graphQueryService, "root", nameof(UserLikesUserEdge), edge.InId, edge.OutId);

            var result = await query.GetAsync(q => q, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GenericGetAsyncWithConfigureFuncReturnsEdge()
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
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var query = CreateGenericQuery(graphQueryService, "root", nameof(UserLikesUserEdge), edge.InId, edge.OutId);

            var result = await query.GetAsync(q => q.WithConsistentRead(true), cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(edge.InId, result.InId);
            Assert.AreEqual(edge.OutId, result.OutId);
        }

        [TestMethod]
        public async Task GenericGetAsyncThrowsWhenEdgeNotFound()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Get
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var nonExistentInId = GlobalId.Get<User>(Guid.NewGuid().ToString());
            var nonExistentOutId = GlobalId.Get<User>(Guid.NewGuid().ToString());
            var query = CreateGenericQuery(graphQueryService, "root", nameof(UserLikesUserEdge), nonExistentInId, nonExistentOutId);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(
                async () => await query.GetAsync(q => q, cancellationToken));
        }
    }
}
