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
using GraphlessDB.Domain.Graph;
using GraphlessDB.Extensions.DependencyInjection;
using GraphlessDB.Query;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public abstract class DatalogQueryTests
    {
        protected abstract IServiceCollection ConfigureGraphDBServices(IServiceCollection services);

        [TestMethod]
        public async Task CanQuerySimpleNodePatternAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var john = User.New("john");
            var jane = User.New("jane");

            // Add users
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(john, jane))
                .ExecuteAsync(cancellationToken);

            // Query using datalog: find user with Username = "john"
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var user)
                .Match(user, m => m.WhereEquals("Username", "john"))
                .Select(user)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(john.Id, results[0].Id);
        }

        [TestMethod]
        public async Task CanQueryEdgePatternsAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var john = User.New("john");
            var jane = User.New("jane");
            var bob = User.New("bob");
            var johnLikesJane = UserLikesUserEdge.New(john, jane);
            var johnLikesBob = UserLikesUserEdge.New(john, bob);

            // Add users and edges
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(john, jane, bob, johnLikesJane, johnLikesBob))
                .ExecuteAsync(cancellationToken);

            // Query: find all users that john likes
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var john2)
                .Var<User>(out var liked)
                .Match(john2, m => m.WhereEquals("Username", "john"))
                .Edge<UserLikesUserEdge, User, User>(john2, liked)
                .Select(liked)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(2, results.Count);
            var likedIds = results.Select(u => u.Id).ToList();
            Assert.IsTrue(likedIds.Contains(jane.Id));
            Assert.IsTrue(likedIds.Contains(bob.Id));
        }

        [TestMethod]
        public async Task CanQueryRecursivePatternAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Create a chain: user0 -> user1 -> user2 -> user3
            var user0 = User.New("user0");
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edge01 = UserLikesUserEdge.New(user0, user1);
            var edge12 = UserLikesUserEdge.New(user1, user2);
            var edge23 = UserLikesUserEdge.New(user2, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user3, edge01, edge12, edge23))
                .ExecuteAsync(cancellationToken);

            // Query: find all users reachable from user0 within 3 hops
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var start)
                .Var<User>(out var reachable)
                .Match(start, m => m.WhereEquals("Username", "user0"))
                .Recursive<UserLikesUserEdge, User>(start, reachable, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(3)
                    .BreadthFirst())
                .Select(reachable)
                .GetAsync(true, cancellationToken);

            // Should find user1, user2, user3
            Assert.AreEqual(3, results.Count);
            var reachedIds = results.Select(u => u.Id).ToList();
            Assert.IsTrue(reachedIds.Contains(user1.Id));
            Assert.IsTrue(reachedIds.Contains(user2.Id));
            Assert.IsTrue(reachedIds.Contains(user3.Id));
        }

        [TestMethod]
        public async Task RecursivePatternRespectsMaxDepthAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Create a chain: user0 -> user1 -> user2 -> user3
            var user0 = User.New("user0");
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edge01 = UserLikesUserEdge.New(user0, user1);
            var edge12 = UserLikesUserEdge.New(user1, user2);
            var edge23 = UserLikesUserEdge.New(user2, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user3, edge01, edge12, edge23))
                .ExecuteAsync(cancellationToken);

            // Query: find all users reachable from user0 within 2 hops only
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var start)
                .Var<User>(out var reachable)
                .Match(start, m => m.WhereEquals("Username", "user0"))
                .Recursive<UserLikesUserEdge, User>(start, reachable, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(2)
                    .BreadthFirst())
                .Select(reachable)
                .GetAsync(true, cancellationToken);

            // Should find user1, user2 (but NOT user3 - too far)
            Assert.AreEqual(2, results.Count);
            var reachedIds = results.Select(u => u.Id).ToList();
            Assert.IsTrue(reachedIds.Contains(user1.Id));
            Assert.IsTrue(reachedIds.Contains(user2.Id));
            Assert.IsFalse(reachedIds.Contains(user3.Id));
        }

        [TestMethod]
        public async Task RecursivePatternRespectsMinDepthAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Create a chain: user0 -> user1 -> user2 -> user3
            var user0 = User.New("user0");
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edge01 = UserLikesUserEdge.New(user0, user1);
            var edge12 = UserLikesUserEdge.New(user1, user2);
            var edge23 = UserLikesUserEdge.New(user2, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user3, edge01, edge12, edge23))
                .ExecuteAsync(cancellationToken);

            // Query: find all users reachable from user0 at depth 2-3 hops (skip depth 1)
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var start)
                .Var<User>(out var reachable)
                .Match(start, m => m.WhereEquals("Username", "user0"))
                .Recursive<UserLikesUserEdge, User>(start, reachable, r => r
                    .Via(e => e)
                    .MinDepth(2)
                    .MaxDepth(3)
                    .BreadthFirst())
                .Select(reachable)
                .GetAsync(true, cancellationToken);

            // Should find user2, user3 (but NOT user1 - too close)
            Assert.AreEqual(2, results.Count);
            var reachedIds = results.Select(u => u.Id).ToList();
            Assert.IsFalse(reachedIds.Contains(user1.Id));
            Assert.IsTrue(reachedIds.Contains(user2.Id));
            Assert.IsTrue(reachedIds.Contains(user3.Id));
        }

        [TestMethod]
        public async Task RecursiveQueryDetectsCyclesAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Create a cycle: user0 -> user1 -> user2 -> user0
            var user0 = User.New("user0");
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var edge01 = UserLikesUserEdge.New(user0, user1);
            var edge12 = UserLikesUserEdge.New(user1, user2);
            var edge20 = UserLikesUserEdge.New(user2, user0); // Cycle back

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, edge01, edge12, edge20))
                .ExecuteAsync(cancellationToken);

            // Query: recursive search should detect cycle and not loop infinitely
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var start)
                .Var<User>(out var reachable)
                .Match(start, m => m.WhereEquals("Username", "user0"))
                .Recursive<UserLikesUserEdge, User>(start, reachable, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10) // Even with high depth, should not infinite loop
                    .BreadthFirst())
                .Select(reachable)
                .GetAsync(true, cancellationToken);

            // Should find all nodes in the cycle exactly once
            Assert.AreEqual(2, results.Count); // user1 and user2 (user0 is excluded as it's the start)
            var reachedIds = results.Select(u => u.Id).ToList();
            Assert.IsTrue(reachedIds.Contains(user1.Id));
            Assert.IsTrue(reachedIds.Contains(user2.Id));
        }

        [TestMethod]
        public async Task CanApplyFiltersToAllOperatorsAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user10 = User.New("User10");
            var user11 = User.New("User11");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user10, user11))
                .ExecuteAsync(cancellationToken);

            // Test WhereIn filter
            var resultsIn = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var userIn)
                .Match(userIn, m => m.WhereIn("Username", "User0", "User1"))
                .Select(userIn)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(2, resultsIn.Count);

            // Test WhereBeginsWith filter
            var resultsBw = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<User>(out var userBw)
                .Match(userBw, m => m.WhereBeginsWith("Username", "User1"))
                .Select(userBw)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(3, resultsBw.Count); // User1, User10, User11
        }

        private ServiceProvider GetServiceProvider()
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

            services.AddTestInstrumentation(Debugger.IsAttached);

            ConfigureGraphDBServices(services);

            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
        }
    }
}
