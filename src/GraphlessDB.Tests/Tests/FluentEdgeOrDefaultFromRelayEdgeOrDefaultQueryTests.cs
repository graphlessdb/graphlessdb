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
    public sealed class FluentEdgeOrDefaultFromRelayEdgeOrDefaultQueryTests
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

        private static FluentEdgeOrDefaultFromRelayEdgeOrDefaultQuery<UserOwnsCarEdge> CreateQuery(IGraphQueryExecutionService service, string key)
        {
            var rootKey = "root";
            var query = ImmutableTree<string, GraphQueryNode>.Empty
                .AddNode(rootKey, new GraphQueryNode(new EdgeOrDefaultByIdQuery(nameof(UserOwnsCarEdge), GlobalId.Get<User>("test"), GlobalId.Get<Car>("test"), false, null)));

            return new FluentEdgeOrDefaultFromRelayEdgeOrDefaultQuery<UserOwnsCarEdge>(
                service,
                query,
                key);
        }

        [TestMethod]
        public async Task GetAsyncWithoutConfigureReturnsNullWhenEdgeNotFound()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Get
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();
            var query = CreateQuery(graphQueryService, "root");

            var edge = await query.GetAsync(q => q, cancellationToken);

            Assert.IsNull(edge);
        }

        [TestMethod]
        public async Task GetAsyncWithoutConfigureReturnsEdge()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var johnsmith = User.New("johnsmith");
            var car = Car.New("Tesla");
            var edge = UserOwnsCarEdge.New(johnsmith.Id, car.Id);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(johnsmith, car, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();

            var rootKey = "root";
            var query = ImmutableTree<string, GraphQueryNode>.Empty
                .AddNode(rootKey, new GraphQueryNode(new EdgeOrDefaultByIdQuery(nameof(UserOwnsCarEdge), johnsmith.Id, car.Id, false, null)));

            var queryObj = new FluentEdgeOrDefaultFromRelayEdgeOrDefaultQuery<UserOwnsCarEdge>(
                graphQueryService,
                query,
                rootKey);

            var result = await queryObj.GetAsync(q => q, cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(johnsmith.Id, result.InId);
            Assert.AreEqual(car.Id, result.OutId);
        }

        [TestMethod]
        public async Task GetAsyncWithConfigureFuncReturnsEdge()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var johnsmith = User.New("johnsmith");
            var car = Car.New("Tesla");
            var edge = UserOwnsCarEdge.New(johnsmith.Id, car.Id);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(johnsmith, car, edge))
                .ExecuteAsync(cancellationToken);

            // Get
            using var scope = services.CreateScope();
            var graphQueryService = scope.ServiceProvider.GetRequiredService<IGraphQueryExecutionService>();

            var rootKey = "root";
            var query = ImmutableTree<string, GraphQueryNode>.Empty
                .AddNode(rootKey, new GraphQueryNode(new EdgeOrDefaultByIdQuery(nameof(UserOwnsCarEdge), johnsmith.Id, car.Id, false, null)));

            var queryObj = new FluentEdgeOrDefaultFromRelayEdgeOrDefaultQuery<UserOwnsCarEdge>(
                graphQueryService,
                query,
                rootKey);

            var result = await queryObj.GetAsync(q => q.WithConsistentRead(true), cancellationToken);

            Assert.IsNotNull(result);
            Assert.AreEqual(johnsmith.Id, result.InId);
            Assert.AreEqual(car.Id, result.OutId);
        }
    }
}
