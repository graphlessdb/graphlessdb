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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Extensions.DependencyInjection;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public abstract class GraphDBTests
    {
        protected abstract IServiceCollection ConfigureGraphDBServices(IServiceCollection services);
        [TestMethod]
        public async Task CanGetNodeByNodeAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var johnsmith = User.New("johnsmith");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(johnsmith)
                .ExecuteAsync(cancellationToken);

            // Get
            var node = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(node);

            // Get
            var nodeByNode = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(node)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(nodeByNode);
        }

        [TestMethod]
        public async Task CanCreateUpdateAndDeleteNodeAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var johnsmith = User.New("johnsmith");

            // Get
            var cantGetPerson = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .UserOrDefault(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNull(cantGetPerson);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(johnsmith)
                .ExecuteAsync(cancellationToken);

            // Get
            var newPerson = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(newPerson);

            // Update
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(newPerson.Update() with { Username = "johnsmith (Updated!)" })
                .ExecuteAsync(cancellationToken);

            // Get
            var updatedPerson = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual("johnsmith (Updated!)", updatedPerson.Username);
            Assert.AreEqual(newPerson.Version.NodeVersion + 1, updatedPerson.Version.NodeVersion);

            // Delete
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(updatedPerson.Delete())
                .ExecuteAsync(cancellationToken);

            // Get
            var cantGetPersonAfterDelete = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .UserOrDefault(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNull(cantGetPersonAfterDelete);

            // GraphlessDBNodeAttribute.CarOrder
        }

        [TestMethod]
        public async Task CanCreateUpdateAndDeleteEdgeAsync()
        {
            // Init
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user4 = User.New("User4");
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user2);
            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user3);
            var user1HasFriendUser4 = UserLikesUserEdge.New(user1, user4);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, user3, user4, user1HasFriendUser2, user1HasFriendUser3, user1HasFriendUser4)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .GetEntitiesAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.AreEqual(3, friends.Count);
            Assert.AreEqual(user2.Id, friends[0].Id);
            Assert.AreEqual(user3.Id, friends[1].Id);
            Assert.AreEqual(user4.Id, friends[2].Id);

            var updatedUser1HasFriendUser2 = user1HasFriendUser2.Update() with { LikesUsername = "User5" };

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(updatedUser1HasFriendUser2)
                .WithAllEdgesCheckForNodes(user1, user2)
                .ExecuteAsync(cancellationToken);

            var reorderedFriends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .GetEntitiesAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.AreEqual(3, reorderedFriends.Count);
            Assert.AreEqual(user3.Id, reorderedFriends[0].Id);
            Assert.AreEqual(user4.Id, reorderedFriends[1].Id);
            Assert.AreEqual(user2.Id, reorderedFriends[2].Id);
        }

        [TestMethod]
        public async Task CannotUpdateStaleNodeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var johnsmith = User.New("johnsmith");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(johnsmith)
                .ExecuteAsync(cancellationToken);

            // Get
            var newPerson = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(newPerson);

            // Update
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(newPerson.Update() with { Username = "johnsmith (Updated!)" })
                .ExecuteAsync(cancellationToken);

            // Get
            var updatedPerson = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual("johnsmith (Updated!)", updatedPerson.Username);
            Assert.AreEqual(newPerson.Version.NodeVersion + 1, updatedPerson.Version.NodeVersion);

            // Attempt update on stale version
            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(async () =>
            {
                await services
                    .CreateScope()
                    .GraphDB()
                    .Graph<TestGraph>()
                    .Put(newPerson.Update() with { Username = "johnsmith (Attempted update!)" })
                    .ExecuteAsync(cancellationToken);
            });
        }

        [TestMethod]
        public async Task MultiNodeCreateAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var johnsmith = User.New("johnsmith");
            var janesmith = User.New("janesmith");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    johnsmith,
                    janesmith))
                .ExecuteAsync(cancellationToken);

            // Get
            var newPerson1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(newPerson1);

            var newPerson2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(janesmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(newPerson2);

            // Attempted create
            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await services
                    .CreateScope()
                    .GraphDB()
                    .Graph<TestGraph>()
                    .Put(johnsmith)
                    .ExecuteAsync(cancellationToken);
            });
        }

        // // ThrowsOnMultiNodeUpdateWithExistingNodeAsync

        // // ThrowsOnMultiNodeUpdateWithStaleNodeAsync

        // // ThrowsOnMultiNodeUpdateWithMissingNodeAsync

        [TestMethod]
        public async Task MultiRelatedNodeUpdateAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var johnsmith = User.New("johnsmith");
            var janesmith = User.New("janesmith");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    johnsmith,
                    janesmith))
                .ExecuteAsync(cancellationToken);

            // Get
            var newPerson1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(newPerson1);

            var newPerson2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(janesmith.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(newPerson2);

            // Relate
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(UserLikesUserEdge.New(johnsmith, janesmith))
                .WithAllEdgesCheckForNodes(johnsmith, janesmith)
                .ExecuteAsync(cancellationToken);

            // Traverse edge
            var friendOfPerson1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .LikesUsers()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(friendOfPerson1);
            Assert.AreEqual("janesmith", friendOfPerson1.Username);

            // Traverse edge in reverse
            var friendOfPerson2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(janesmith.Id)
                .LikedByUsers()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.IsNotNull(friendOfPerson2);
            Assert.AreEqual("johnsmith", friendOfPerson2.Username);

            // Delete edge
            var hasFriendEdge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(janesmith.Id)
                .LikedByUsersEdges()
                .Single()
                .GetAsync(true, cancellationToken);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(hasFriendEdge.Delete())
                .WithNoEdgeChecksForAllNodes()
                .ExecuteAsync(cancellationToken);

            // Attempt traverse edge
            var noFriendOfPerson1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(johnsmith.Id)
                .LikesUsers()
                .SingleOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNull(noFriendOfPerson1);

            // Attempt traverse edge in reverse
            var noFriendOfPerson2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .User(janesmith.Id)
                .LikedByUsers()
                .SingleOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNull(noFriendOfPerson2);

            // Attempted create
            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await services
                    .CreateScope()
                    .GraphDB()
                    .Graph<TestGraph>()
                    .Put(johnsmith)
                    .ExecuteAsync(cancellationToken);
            });
        }

        [TestMethod]
        public async Task CanGetEdgeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0HasFriendUser1))
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Edge<UserLikesUserEdge, User, User>(user0.Id, user1.Id)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user0HasFriendUser1, edge);
        }

        [TestMethod]
        public async Task CanGetEdgeOrDefaultAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1))
                .ExecuteAsync(cancellationToken);

            var edge1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .EdgeOrDefault<UserLikesUserEdge, User, User>(user0.Id, user1.Id)
                .GetAsync(true, cancellationToken);

            Assert.IsNull(edge1);

            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user0HasFriendUser1)
                .WithNoEdgeChecksForAllNodes()
                .ExecuteAsync(cancellationToken);

            var edge2 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .EdgeOrDefault<UserLikesUserEdge, User, User>(user0.Id, user1.Id)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user0HasFriendUser1, edge2);
        }

        [TestMethod]
        public async Task CanGetSingleToEdgeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0HasFriendUser1))
                .ExecuteAsync(cancellationToken);

            var outUser = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user0)
                .LikesUsers()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user1, outUser);

            var inUser = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikedByUsers()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user0, inUser);
        }

        [TestMethod]
        public async Task CanGetInToAllEdgeTypesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var car0 = Car.New("Car0");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);
            var user0HasCar0 = UserOwnsCarEdge.New(user0.Id, car0.Id);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, car0, user0HasFriendUser1, user0HasCar0))
                .ExecuteAsync(cancellationToken);

            var allEdges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user0)
                .InToEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.AreEqual(2, allEdges.Edges.Count);

            var allNodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user0)
                .InToEdges()
                .OutFromEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.AreEqual(2, allNodes.Edges.Count);
        }

        [TestMethod]
        public async Task CanGetOutToAllEdgeTypesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var car0 = Car.New("Car0");
            var manufacturer0 = Manufacturer.New("Manufacturer0");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);
            var user0HasCar0 = UserOwnsCarEdge.New(user0.Id, car0.Id);
            var manufacturer0MakesCar0 = ManufacturerMakesCarEdge.New(manufacturer0.Id, car0.Id);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, car0, manufacturer0, user0HasFriendUser1, user0HasCar0, manufacturer0MakesCar0))
                .ExecuteAsync(cancellationToken);

            var allEdges = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(car0)
                .OutToEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.AreEqual(2, allEdges.Edges.Count);

            var allNodes = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(car0)
                .OutToEdges()
                .InFromEdges()
                .GetAsync(true, ConnectionArguments.FirstMax, cancellationToken);

            Assert.AreEqual(2, allNodes.Edges.Count);
        }

        [TestMethod]
        public async Task CanGetSingleEdgeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user0HasFriendUser1))
                .ExecuteAsync(cancellationToken);

            var resultUser1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .LikesUsers()
                .Single()
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user1, resultUser1);
        }

        [TestMethod]
        public async Task ThrowsOnGetSingleEdgeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);
            var user0HasFriendUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0HasFriendUser1, user0HasFriendUser2))
                .ExecuteAsync(cancellationToken);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(
                () => services
                    .CreateScope()
                    .GraphDB()
                    .Graph<TestGraph>()
                    .Users()
                    .LikesUsers()
                    .Single()
                    .GetAsync(true, cancellationToken));
        }

        [TestMethod]
        public async Task CanGetFirstEdgeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user0HasFriendUser1 = UserLikesUserEdge.New(user0, user1);
            var user0HasFriendUser2 = UserLikesUserEdge.New(user0, user2);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user0HasFriendUser1, user0HasFriendUser2))
                .ExecuteAsync(cancellationToken);

            var resultUser1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsers(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .First()
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user1, resultUser1);
        }

        [TestMethod]
        public async Task CanGetSingleOrDefaultEdgeWhereNoNodesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var resultUser1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .LikesUsers()
                .SingleOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNull(resultUser1);
        }

        [TestMethod]
        public async Task CanGetSingleOrDefaultEdgeWhereNoEdgesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2))
                .ExecuteAsync(cancellationToken);

            var resultUser1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsers()
                .SingleOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNull(resultUser1);
        }


        [TestMethod]
        public async Task CanGetFirstOrDefaultEdgeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2))
                .ExecuteAsync(cancellationToken);

            var resultUser1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsers()
                .FirstOrDefault()
                .GetAsync(true, cancellationToken);

            Assert.IsNull(resultUser1);
        }

        [TestMethod]
        public async Task CanFetchNodesUsingCursorAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user4 = User.New("User4");

            var usersToFind = ImmutableHashSet.Create(
                user0.Username, user1.Username,
                user2.Username, user3.Username,
                user4.Username);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user3, user4))
                .ExecuteAsync(cancellationToken);

            // Get first two
            var firstTwoUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .GetAsync(true, ConnectionArguments.GetFirst(2), cancellationToken);

            Assert.AreEqual(2, firstTwoUsers.Edges.Count);
            Assert.IsTrue(usersToFind.Contains(firstTwoUsers.Edges[0].Node.Username));
            usersToFind = usersToFind.Remove(firstTwoUsers.Edges[0].Node.Username);
            Assert.IsTrue(usersToFind.Contains(firstTwoUsers.Edges[1].Node.Username));
            usersToFind = usersToFind.Remove(firstTwoUsers.Edges[1].Node.Username);
            Assert.IsFalse(firstTwoUsers.PageInfo.HasPreviousPage);
            Assert.IsTrue(firstTwoUsers.PageInfo.HasNextPage);

            var secondTwoUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .GetAsync(true, ConnectionArguments.GetFirst(2, firstTwoUsers.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(2, secondTwoUsers.Edges.Count);
            Assert.IsTrue(usersToFind.Contains(secondTwoUsers.Edges[0].Node.Username));
            usersToFind = usersToFind.Remove(secondTwoUsers.Edges[0].Node.Username);
            Assert.IsTrue(usersToFind.Contains(secondTwoUsers.Edges[1].Node.Username));
            usersToFind = usersToFind.Remove(secondTwoUsers.Edges[1].Node.Username);
            Assert.IsFalse(secondTwoUsers.PageInfo.HasPreviousPage);
            Assert.IsTrue(secondTwoUsers.PageInfo.HasNextPage);

            var lastUser = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .GetAsync(true, ConnectionArguments.GetFirst(2, secondTwoUsers.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(1, lastUser.Edges.Count);
            Assert.IsTrue(usersToFind.Contains(lastUser.Edges[0].Node.Username));
            usersToFind = usersToFind.Remove(lastUser.Edges[0].Node.Username);
            Assert.IsFalse(lastUser.PageInfo.HasPreviousPage);
            Assert.IsFalse(lastUser.PageInfo.HasNextPage);
            Assert.IsTrue(usersToFind.IsEmpty);
        }

        [TestMethod]
        public async Task CanFetchOrderedNodesUsingCursorAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user4 = User.New("User4");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user0, user1, user2, user3, user4))
                .ExecuteAsync(cancellationToken);

            // Get first two
            var firstTwoUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .GetAsync(true, ConnectionArguments.GetFirst(2), cancellationToken);

            Assert.AreEqual(2, firstTwoUsers.Edges.Count);
            Assert.IsFalse(firstTwoUsers.PageInfo.HasPreviousPage);
            Assert.IsTrue(firstTwoUsers.PageInfo.HasNextPage);
            Assert.AreEqual("User0", firstTwoUsers.Edges[0].Node.Username);
            Assert.AreEqual("User1", firstTwoUsers.Edges[1].Node.Username);

            var secondTwoUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .GetAsync(true, ConnectionArguments.GetFirst(2, firstTwoUsers.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(2, secondTwoUsers.Edges.Count);
            Assert.IsFalse(secondTwoUsers.PageInfo.HasPreviousPage);
            Assert.IsTrue(secondTwoUsers.PageInfo.HasNextPage);
            Assert.AreEqual("User2", secondTwoUsers.Edges[0].Node.Username);
            Assert.AreEqual("User3", secondTwoUsers.Edges[1].Node.Username);

            var lastUser = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .GetAsync(true, ConnectionArguments.GetFirst(2, secondTwoUsers.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(1, lastUser.Edges.Count);
            Assert.IsFalse(lastUser.PageInfo.HasPreviousPage);
            Assert.IsFalse(lastUser.PageInfo.HasNextPage);
            Assert.AreEqual("User4", lastUser.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task CanFetchNodesWithBeginsWithAnyFilterAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user00 = User.New("User00");
            var user01 = User.New("User01");
            var user02 = User.New("User02");
            var user10 = User.New("User10");
            var user11 = User.New("User11");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user00, user01, user02, user10, user11))
                .ExecuteAsync(cancellationToken);

            // Get first two beginning with User1
            var userConnection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(
                    new UserOrder { Username = OrderDirection.Asc },
                    new UserFilter { Username = new StringFilter { BeginsWithAny = ["User0", "User1"] } })
                .GetAsync(true, ConnectionArguments.GetFirst(4), 1, 1, cancellationToken);

            Assert.AreEqual(4, userConnection.Edges.Count);
            Assert.IsFalse(userConnection.PageInfo.HasPreviousPage);
            Assert.IsTrue(userConnection.PageInfo.HasNextPage);
            Assert.AreEqual("User00", userConnection.Edges[0].Node.Username);
            Assert.AreEqual("User01", userConnection.Edges[1].Node.Username);
            Assert.AreEqual("User02", userConnection.Edges[2].Node.Username);
            Assert.AreEqual("User10", userConnection.Edges[3].Node.Username);
        }

        [TestMethod]
        public async Task CanFetchOrderedAndFilteredNodesUsingCursorAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user00 = User.New("User00");
            var user01 = User.New("User01");
            var user02 = User.New("User02");
            var user10 = User.New("User10");
            var user11 = User.New("User11");

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user00, user01, user02, user10, user11))
                .ExecuteAsync(cancellationToken);

            // Get first two beginning with User1
            var firstTwoUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(
                    new UserOrder { Username = OrderDirection.Asc },
                    new UserFilter { Username = new StringFilter { BeginsWith = "User1" } })
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(2, firstTwoUsers.Edges.Count);
            Assert.IsFalse(firstTwoUsers.PageInfo.HasPreviousPage);
            Assert.IsFalse(firstTwoUsers.PageInfo.HasNextPage);
            Assert.AreEqual("User10", firstTwoUsers.Edges[0].Node.Username);
            Assert.AreEqual("User11", firstTwoUsers.Edges[1].Node.Username);

            // Get first two beginning with User0
            var firstTwoUsersWithUser0 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(
                    new UserOrder { Username = OrderDirection.Asc },
                    new UserFilter { Username = new StringFilter { BeginsWith = "User0" } })
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(2, firstTwoUsersWithUser0.Edges.Count);
            Assert.IsFalse(firstTwoUsersWithUser0.PageInfo.HasPreviousPage);
            Assert.IsTrue(firstTwoUsersWithUser0.PageInfo.HasNextPage);
            Assert.AreEqual("User00", firstTwoUsersWithUser0.Edges[0].Node.Username);
            Assert.AreEqual("User01", firstTwoUsersWithUser0.Edges[1].Node.Username);
        }

        [TestMethod]
        public async Task ThrowsWhenUsingMultipleOrderingAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user00 = User.New("User00");
            var user01 = User.New("User01");
            var user02 = User.New("User02");
            var user10 = User.New("User10");
            var user11 = User.New("User11");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user00, user01, user02, user10, user11))
                .ExecuteAsync(cancellationToken);

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(
                async () => await services
                    .CreateScope()
                    .GraphDB()
                    .Graph<TestGraph>()
                    .Users(new UserOrder { Id = OrderDirection.Asc, Username = OrderDirection.Asc, })
                    .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken));
        }

        [TestMethod]
        public async Task CanOrderAndFilterOnDifferentPropertiesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user00 = User.New("User00");
            var user01 = User.New("User01");
            var user02 = User.New("User02");
            var user10 = User.New("User10");
            var user11 = User.New("User11");

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(user00, user01, user02, user10, user11))
                .ExecuteAsync(cancellationToken);

            var users = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(
                    new UserOrder { Username = OrderDirection.Asc, },
                    new UserFilter { Id = new IdFilter { Eq = user02.Id } })
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(1, users.Edges.Count);
            Assert.AreEqual("User02", users.Edges[0].Node.Username);
        }

        [TestMethod]
        public async Task CanPageThroughEdgeTraversalAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var car00 = Car.New("Car00");
            var car01 = Car.New("Car01");
            var car10 = Car.New("Car10");
            var car11 = Car.New("Car11");
            var user0HasCar00 = UserOwnsCarEdge.New(user0.Id, car00.Id);
            var user0HasCar01 = UserOwnsCarEdge.New(user0.Id, car01.Id);
            var user1HasCar10 = UserOwnsCarEdge.New(user1.Id, car10.Id);
            var user1HasCar11 = UserOwnsCarEdge.New(user1.Id, car11.Id);

            var carsToFind = ImmutableHashSet.Create(
                car00.Model, car01.Model,
                car10.Model, car11.Model);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    user0, user1, car00, car01, car10, car11,
                    user0HasCar00, user0HasCar01, user1HasCar10, user1HasCar11))
                .ExecuteAsync(cancellationToken);

            var firstTwoCars = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .OwnsCars()
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(2, firstTwoCars.Edges.Count);
            Assert.IsTrue((firstTwoCars.Edges[0].Node.Model == "Car00" || firstTwoCars.Edges[0].Node.Model == "Car01") && carsToFind.Contains(firstTwoCars.Edges[0].Node.Model));
            carsToFind = carsToFind.Remove(firstTwoCars.Edges[0].Node.Model);
            Assert.IsTrue((firstTwoCars.Edges[1].Node.Model == "Car00" || firstTwoCars.Edges[1].Node.Model == "Car01") && carsToFind.Contains(firstTwoCars.Edges[1].Node.Model));
            carsToFind = carsToFind.Remove(firstTwoCars.Edges[1].Node.Model);
            Assert.IsFalse(firstTwoCars.PageInfo.HasPreviousPage);
            Assert.IsTrue(firstTwoCars.PageInfo.HasNextPage);

            var secondTwoCars = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .OwnsCars()
                .GetAsync(true, ConnectionArguments.GetFirst(2, firstTwoCars.PageInfo.EndCursor), 1, 1, cancellationToken);

            Assert.AreEqual(2, secondTwoCars.Edges.Count);
            Assert.IsTrue((secondTwoCars.Edges[0].Node.Model == "Car10" || secondTwoCars.Edges[0].Node.Model == "Car11") && carsToFind.Contains(secondTwoCars.Edges[0].Node.Model));
            carsToFind = carsToFind.Remove(secondTwoCars.Edges[0].Node.Model);
            Assert.IsTrue((secondTwoCars.Edges[0].Node.Model == "Car10" || secondTwoCars.Edges[0].Node.Model == "Car11") && carsToFind.Contains(secondTwoCars.Edges[1].Node.Model));
            carsToFind = carsToFind.Remove(secondTwoCars.Edges[1].Node.Model);
            Assert.IsFalse(secondTwoCars.PageInfo.HasPreviousPage);
            Assert.IsFalse(secondTwoCars.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanFetchEdgesUsingCursorAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user4 = User.New("User4");
            var user5 = User.New("User5");
            var user6 = User.New("User6");
            var user7 = User.New("User7");
            var user4HasFriendUser5 = UserLikesUserEdge.New(user4, user5);
            var user4HasFriendUser6 = UserLikesUserEdge.New(user4, user6);
            var user4HasFriendUser7 = UserLikesUserEdge.New(user4, user7);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    user0, user1, user2, user3,
                    user4, user5, user6, user7,
                    user4HasFriendUser5, user4HasFriendUser6, user4HasFriendUser7))
                .ExecuteAsync(cancellationToken);

            // Get first two friends
            var firstTwoUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsers()
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(2, firstTwoUsers.Edges.Count);
            Assert.IsFalse(firstTwoUsers.PageInfo.HasPreviousPage);
            Assert.IsTrue(firstTwoUsers.PageInfo.HasNextPage);
            // Assert.AreEqual("User5", firstTwoUsers.Edges[0].Node.UserName);
            // Assert.AreEqual("User6", firstTwoUsers.Edges[1].Node.UserName);

            // Get last friend
            var lastUser = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsers()
                .GetAsync(true, ConnectionArguments.GetFirst(2, firstTwoUsers.PageInfo.EndCursor), 1, 1, cancellationToken);

            Assert.AreEqual(1, lastUser.Edges.Count);
            Assert.IsFalse(lastUser.PageInfo.HasPreviousPage);
            Assert.IsFalse(lastUser.PageInfo.HasNextPage);
            // Assert.AreEqual("User7", lastUser.Edges[0].Node.UserName);
        }

        // [TestMethod]
        public async Task CanFetchInAndOutEdgesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user4 = User.New("User4");
            var user5 = User.New("User5");
            var user6 = User.New("User6");
            var user7 = User.New("User7");
            var user4HasFriendUser5 = UserLikesUserEdge.New(user4, user5);
            var user4HasFriendUser6 = UserLikesUserEdge.New(user4, user6);
            var user4HasFriendUser7 = UserLikesUserEdge.New(user4, user7);
            var user0HasFriendUser4 = UserLikesUserEdge.New(user0, user4);
            var user1HasFriendUser4 = UserLikesUserEdge.New(user1, user4);
            var user2HasFriendUser4 = UserLikesUserEdge.New(user2, user4);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    user0, user1, user2, user3,
                    user4, user5, user6, user7,
                    user4HasFriendUser5, user4HasFriendUser6, user4HasFriendUser7))
                .ExecuteAsync(cancellationToken);

            var hasFriendEdgeConnection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user4)
                .LikedByUsersAndLikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .GetAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(6, hasFriendEdgeConnection.Edges.Count);
            Assert.AreEqual("User4", hasFriendEdgeConnection.Edges[0].Node.LikesUsername);
            Assert.AreEqual("User4", hasFriendEdgeConnection.Edges[1].Node.LikesUsername);
            Assert.AreEqual("User4", hasFriendEdgeConnection.Edges[2].Node.LikesUsername);
            Assert.AreEqual("User5", hasFriendEdgeConnection.Edges[3].Node.LikesUsername);
            Assert.AreEqual("User6", hasFriendEdgeConnection.Edges[4].Node.LikesUsername);
            Assert.AreEqual("User7", hasFriendEdgeConnection.Edges[5].Node.LikesUsername);

            var users = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikedByUsersAndLikesUsers()
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, users.Count);
            Assert.AreEqual("User0", users[0].Username);
            Assert.AreEqual("User4", users[1].Username);
        }

        [TestMethod]
        public async Task CanTraverseMultipleEdgesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user0 = User.New("User0");
            var user1 = User.New("User1");

            var user2 = User.New("User2");
            var user3 = User.New("User3");

            var user4 = User.New("User4");
            var user5 = User.New("User5");

            var user6 = User.New("User6");
            var user7 = User.New("User7");

            var user0HasFriendUser2 = UserLikesUserEdge.New(user0, user2);
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user2);
            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user3);
            var user2HasFriendUser4 = UserLikesUserEdge.New(user2, user4);
            var user3HasFriendUser5 = UserLikesUserEdge.New(user3, user5);
            var user4HasFriendUser6 = UserLikesUserEdge.New(user4, user6);
            var user4HasFriendUser7 = UserLikesUserEdge.New(user4, user7);
            var user5HasFriendUser6 = UserLikesUserEdge.New(user5, user6);
            var user5HasFriendUser7 = UserLikesUserEdge.New(user5, user7);

            // Add
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    user0, user1, user2, user3,
                    user4, user5, user6, user7,
                    user0HasFriendUser2, user1HasFriendUser2, user1HasFriendUser3,
                    user2HasFriendUser4, user3HasFriendUser5, user4HasFriendUser6,
                    user4HasFriendUser7, user5HasFriendUser6, user5HasFriendUser7))
                .ExecuteAsync(cancellationToken);

            var users = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers()
                .LikesUsers()
                .LikesUsers()
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(2, users.Edges.Count);

            var usersReverse = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user7)
                .LikedByUsers()
                .LikedByUsers()
                .LikedByUsers()
                .GetAsync(true, ConnectionArguments.GetFirst(2), 1, 1, cancellationToken);

            Assert.AreEqual(2, usersReverse.Edges.Count);
        }

        [TestMethod]
        public async Task CanDoZipAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();
            var cursorSerializer = services.GetRequiredService<IGraphCursorSerializationService>();

            var usersA = Enumerable.Range(0, 5).Select(i => User.New($"UserA{i}")).ToImmutableList();
            var usersB = Enumerable.Range(0, 5).Select(i => User.New($"UserB{i}")).ToImmutableList();
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(usersA.AddRange(usersB))
                .ExecuteAsync(cancellationToken);

            var query1 = services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(
                    new UserOrder { Username = OrderDirection.Asc },
                    new UserFilter { Username = new StringFilter { BeginsWith = "UserA" } });

            var query2 = services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(
                    new UserOrder { Username = OrderDirection.Asc },
                    new UserFilter { Username = new StringFilter { BeginsWith = "UserB" } });

            var zipResult1 = await query1
                .Zip(query2)
                .GetAsync(true, ConnectionArguments.GetFirst(5), 2, 2, cancellationToken);

            var zipResult1Usernames = zipResult1
                .Edges
                .Select(e => e.Node.Username)
                .ToImmutableList();

            var zipResult1Cursors = zipResult1
                .Edges
                .Select(e => cursorSerializer.Deserialize(e.Cursor))
                .ToImmutableList();

            Assert.AreEqual(5, zipResult1.Edges.Count);
            Assert.AreEqual("UserA0", zipResult1Usernames[0]);
            Assert.AreEqual("UserB0", zipResult1Usernames[1]);
            Assert.AreEqual("UserA1", zipResult1Usernames[2]);
            Assert.AreEqual("UserB1", zipResult1Usernames[3]);
            Assert.AreEqual("UserA2", zipResult1Usernames[4]);

            var usernamesById = usersA
                .Concat(usersB)
                .ToImmutableDictionary(k => k.Id, v => v.Username);

            Assert.AreEqual(0, zipResult1Cursors[0].GetRootNode().Indexed?.Index);
            Assert.AreEqual("UserA0", usernamesById[zipResult1Cursors[0].GetChildNodes(zipResult1Cursors[0].GetRootKey())[0].HasProp?.Subject ?? throw new InvalidOperationException()]);
            Assert.AreEqual("UserB0", usernamesById[zipResult1Cursors[0].GetChildNodes(zipResult1Cursors[0].GetRootKey())[1].HasProp?.Subject ?? throw new InvalidOperationException()]);

            Assert.AreEqual(1, zipResult1Cursors[1].GetRootNode().Indexed?.Index);
            Assert.AreEqual("UserA0", usernamesById[zipResult1Cursors[1].GetChildNodes(zipResult1Cursors[1].GetRootKey())[0].HasProp?.Subject ?? throw new InvalidOperationException()]);
            Assert.AreEqual("UserB0", usernamesById[zipResult1Cursors[1].GetChildNodes(zipResult1Cursors[1].GetRootKey())[1].HasProp?.Subject ?? throw new InvalidOperationException()]);

            Assert.AreEqual(0, zipResult1Cursors[2].GetRootNode().Indexed?.Index);
            Assert.AreEqual("UserA1", usernamesById[zipResult1Cursors[2].GetChildNodes(zipResult1Cursors[2].GetRootKey())[0].HasProp?.Subject ?? throw new InvalidOperationException()]);
            Assert.AreEqual("UserB1", usernamesById[zipResult1Cursors[2].GetChildNodes(zipResult1Cursors[2].GetRootKey())[1].HasProp?.Subject ?? throw new InvalidOperationException()]);

            Assert.AreEqual(1, zipResult1Cursors[3].GetRootNode().Indexed?.Index);
            Assert.AreEqual("UserA1", usernamesById[zipResult1Cursors[3].GetChildNodes(zipResult1Cursors[3].GetRootKey())[0].HasProp?.Subject ?? throw new InvalidOperationException()]);
            Assert.AreEqual("UserB1", usernamesById[zipResult1Cursors[3].GetChildNodes(zipResult1Cursors[3].GetRootKey())[1].HasProp?.Subject ?? throw new InvalidOperationException()]);

            Assert.AreEqual(0, zipResult1Cursors[4].GetRootNode().Indexed?.Index);
            Assert.AreEqual("UserA2", usernamesById[zipResult1Cursors[4].GetChildNodes(zipResult1Cursors[4].GetRootKey())[0].HasProp?.Subject ?? throw new InvalidOperationException()]);
            Assert.AreEqual("UserB2", usernamesById[zipResult1Cursors[4].GetChildNodes(zipResult1Cursors[4].GetRootKey())[1].HasProp?.Subject ?? throw new InvalidOperationException()]);

            var zipResult2 = await query1
                .Zip(query2)
                .GetAsync(true, ConnectionArguments.GetFirst(5, zipResult1.PageInfo.EndCursor), 2, 2, cancellationToken);

            Assert.AreEqual(5, zipResult2.Edges.Count);
            Assert.AreEqual("UserB2", zipResult2.Edges[0].Node.Username);
            Assert.AreEqual("UserA3", zipResult2.Edges[1].Node.Username);
            Assert.AreEqual("UserB3", zipResult2.Edges[2].Node.Username);
            Assert.AreEqual("UserA4", zipResult2.Edges[3].Node.Username);
            Assert.AreEqual("UserB4", zipResult2.Edges[4].Node.Username);
        }

        [TestMethod]
        public async Task CanUseWhereNodeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 7)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var evenUserConnection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => int.Parse(v.Item.Username, CultureInfo.InvariantCulture) % 2 == 0)
                .GetAsync(true, ConnectionArguments.GetFirst(2), cancellationToken);

            Assert.AreEqual(2, evenUserConnection.Edges.Count);
            Assert.AreEqual("0", evenUserConnection.Edges[0].Node.Username);
            Assert.AreEqual("2", evenUserConnection.Edges[1].Node.Username);
            Assert.IsTrue(evenUserConnection.PageInfo.HasNextPage);

            var endCursor = services
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphCursorSerializationService>()
                .Deserialize(evenUserConnection.PageInfo.EndCursor);

            Assert.AreEqual(2, endCursor.GetNodeCount());

            evenUserConnection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => int.Parse(v.Item.Username, CultureInfo.InvariantCulture) % 2 == 0)
                .GetAsync(true, ConnectionArguments.GetFirst(2, evenUserConnection.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(2, evenUserConnection.Edges.Count);
            Assert.AreEqual("4", evenUserConnection.Edges[0].Node.Username);
            Assert.AreEqual("6", evenUserConnection.Edges[1].Node.Username);
            Assert.IsFalse(evenUserConnection.PageInfo.HasNextPage);

            var oddUserConnection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => int.Parse(v.Item.Username, CultureInfo.InvariantCulture) % 2 != 0)
                .GetAsync(true, ConnectionArguments.GetFirst(2), cancellationToken);

            Assert.AreEqual(2, oddUserConnection.Edges.Count);
            Assert.AreEqual("1", oddUserConnection.Edges[0].Node.Username);
            Assert.AreEqual("3", oddUserConnection.Edges[1].Node.Username);
            Assert.IsTrue(oddUserConnection.PageInfo.HasNextPage);

            oddUserConnection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => int.Parse(v.Item.Username, CultureInfo.InvariantCulture) % 2 != 0)
                .GetAsync(true, ConnectionArguments.GetFirst(2, oddUserConnection.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(1, oddUserConnection.Edges.Count);
            Assert.AreEqual("5", oddUserConnection.Edges[0].Node.Username);
            Assert.IsFalse(oddUserConnection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanUseWhereSingleNodeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 10)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => v.Item.Username == "8")
                .Single()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("8", user.Username);
        }

        [TestMethod]
        public async Task CanUseWhereSingleOrDefaultNodeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 10)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => v.Item.Username == "8")
                .SingleOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("8", user?.Username);

            user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => v.Item.Username == "???")
                .SingleOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNull(user);
        }

        [TestMethod]
        public async Task CanUseWhereFirstNodeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 10)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => v.Item.Username == "8")
                .First()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("8", user.Username);
        }

        [TestMethod]
        public async Task CanUseWhereFirstOrDefaultNodeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 10)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => v.Item.Username == "8")
                .FirstOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("8", user?.Username);

            user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .WhereNode(v => v.Item.Username == "???")
                .FirstOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNull(user);
        }

        [TestMethod]
        public async Task CanUseWhereFirstNodeUnSortedQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 10)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .WhereNode(v => v.Item.Username == "8")
                .First()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("8", user.Username);
        }

        [TestMethod]
        public async Task CanUseWhereFirstOrDefaultNodeUnSortedQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 10)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .WhereNode(v => v.Item.Username == "8")
                .FirstOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("8", user?.Username);

            user = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .WhereNode(v => v.Item.Username == "???")
                .FirstOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNull(user);
        }

        [TestMethod]
        public async Task CanUseWhereEdgeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 7)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            var edges = Enumerable
                .Range(0, 7)
                .Select(i =>
                {
                    return ImmutableList.Create(
                        UserLikesUserEdge.New(users[i % 7], users[(i + 1) % 7]),
                        UserLikesUserEdge.New(users[i % 7], users[(i + 2) % 7]),
                        UserLikesUserEdge.New(users[i % 7], users[(i + 3) % 7]));
                })
                .SelectMany(v => v)
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put([.. users, .. edges])
                .ExecuteAsync(cancellationToken);

            var connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => int.Parse(v.Item.LikedByUsername, CultureInfo.InvariantCulture) % 2 == 0)
                .GetAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(10, connection.Edges.Count);
            Assert.AreEqual("0", connection.Edges[0].Node.LikedByUsername);
            Assert.AreEqual("1", connection.Edges[0].Node.LikesUsername);
            Assert.AreEqual("0", connection.Edges[1].Node.LikedByUsername);
            Assert.AreEqual("2", connection.Edges[1].Node.LikesUsername);
            Assert.AreEqual("0", connection.Edges[2].Node.LikedByUsername);
            Assert.AreEqual("3", connection.Edges[2].Node.LikesUsername);
            Assert.AreEqual("2", connection.Edges[3].Node.LikedByUsername);
            Assert.AreEqual("3", connection.Edges[3].Node.LikesUsername);
            Assert.AreEqual("2", connection.Edges[4].Node.LikedByUsername);
            Assert.AreEqual("4", connection.Edges[4].Node.LikesUsername);
            Assert.AreEqual("2", connection.Edges[5].Node.LikedByUsername);
            Assert.AreEqual("5", connection.Edges[5].Node.LikesUsername);
            Assert.AreEqual("4", connection.Edges[6].Node.LikedByUsername);
            Assert.AreEqual("0", connection.Edges[6].Node.LikesUsername);
            Assert.AreEqual("4", connection.Edges[7].Node.LikedByUsername);
            Assert.AreEqual("5", connection.Edges[7].Node.LikesUsername);
            Assert.AreEqual("4", connection.Edges[8].Node.LikedByUsername);
            Assert.AreEqual("6", connection.Edges[8].Node.LikesUsername);
            Assert.AreEqual("6", connection.Edges[9].Node.LikedByUsername);
            Assert.AreEqual("0", connection.Edges[9].Node.LikesUsername);
            Assert.IsTrue(connection.PageInfo.HasNextPage);

            connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => int.Parse(v.Item.LikedByUsername, CultureInfo.InvariantCulture) % 2 == 0)
                .GetAsync(true, ConnectionArguments.GetFirst(2), cancellationToken);

            Assert.AreEqual(2, connection.Edges.Count);
            Assert.AreEqual("0", connection.Edges[0].Node.LikedByUsername);
            Assert.AreEqual("1", connection.Edges[0].Node.LikesUsername);
            Assert.AreEqual("0", connection.Edges[1].Node.LikedByUsername);
            Assert.AreEqual("2", connection.Edges[1].Node.LikesUsername);
            Assert.IsTrue(connection.PageInfo.HasNextPage);

            connection = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => int.Parse(v.Item.LikedByUsername, CultureInfo.InvariantCulture) % 2 == 0)
                .GetAsync(true, ConnectionArguments.GetFirst(2, connection.PageInfo.EndCursor), cancellationToken);

            Assert.AreEqual(2, connection.Edges.Count);
            Assert.AreEqual("0", connection.Edges[0].Node.LikedByUsername);
            Assert.AreEqual("3", connection.Edges[0].Node.LikesUsername);
            Assert.AreEqual("2", connection.Edges[1].Node.LikedByUsername);
            Assert.AreEqual("3", connection.Edges[1].Node.LikesUsername);
            Assert.IsTrue(connection.PageInfo.HasNextPage);
        }

        [TestMethod]
        public async Task CanUseWhereSingleEdgeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 7)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            var edges = Enumerable
                .Range(0, 7)
                .Select(i =>
                {
                    return ImmutableList.Create(
                        UserLikesUserEdge.New(users[i % 7], users[(i + 1) % 7]),
                        UserLikesUserEdge.New(users[i % 7], users[(i + 2) % 7]));
                })
                .SelectMany(v => v)
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put([.. users, .. edges])
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc }, new UserFilter { Username = new StringFilter { Eq = "0" } })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => v.Item.LikesUsername == "1")
                .Single()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("0", edge.LikedByUsername);
            Assert.AreEqual("1", edge.LikesUsername);
        }

        [TestMethod]
        public async Task CanUseWhereSingleOrDefaultEdgeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 7)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            var edges = Enumerable
                .Range(0, 7)
                .Select(i =>
                {
                    return ImmutableList.Create(
                        UserLikesUserEdge.New(users[i % 7], users[(i + 1) % 7]),
                        UserLikesUserEdge.New(users[i % 7], users[(i + 2) % 7]));
                })
                .SelectMany(v => v)
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put([.. users, .. edges])
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc }, new UserFilter { Username = new StringFilter { Eq = "0" } })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => v.Item.LikesUsername == "1")
                .SingleOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual("0", edge.LikedByUsername);
            Assert.AreEqual("1", edge.LikesUsername);

            edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc }, new UserFilter { Username = new StringFilter { Eq = "0" } })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => v.Item.LikesUsername == "???")
                .SingleOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNull(edge);
        }


        [TestMethod]
        public async Task CanUseWhereFirstEdgeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 7)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            var edges = Enumerable
                .Range(0, 7)
                .Select(i =>
                {
                    return ImmutableList.Create(
                        UserLikesUserEdge.New(users[i % 7], users[(i + 1) % 7]),
                        UserLikesUserEdge.New(users[i % 7], users[(i + 2) % 7]));
                })
                .SelectMany(v => v)
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put([.. users, .. edges])
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc }, new UserFilter { Username = new StringFilter { Eq = "0" } })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => v.Item.LikesUsername == "1")
                .First()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.AreEqual("0", edge.LikedByUsername);
            Assert.AreEqual("1", edge.LikesUsername);
        }

        [TestMethod]
        public async Task CanUseWhereFirstOrDefaultEdgeQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var users = Enumerable
                .Range(0, 7)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            var edges = Enumerable
                .Range(0, 7)
                .Select(i =>
                {
                    return ImmutableList.Create(
                        UserLikesUserEdge.New(users[i % 7], users[(i + 1) % 7]),
                        UserLikesUserEdge.New(users[i % 7], users[(i + 2) % 7]));
                })
                .SelectMany(v => v)
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put([.. users, .. edges])
                .ExecuteAsync(cancellationToken);

            var edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc }, new UserFilter { Username = new StringFilter { Eq = "0" } })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => v.Item.LikesUsername == "1")
                .FirstOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNotNull(edge);
            Assert.AreEqual("0", edge.LikedByUsername);
            Assert.AreEqual("1", edge.LikesUsername);

            edge = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users(new UserOrder { Username = OrderDirection.Asc }, new UserFilter { Username = new StringFilter { Eq = "0" } })
                .LikesUsersEdges(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .WhereEdge(v => v.Item.LikesUsername == "???")
                .FirstOrDefault()
                .GetAsync(true, 2, 2, cancellationToken);

            Assert.IsNull(edge);
        }

        [TestMethod]
        public async Task CanTraverseInEdgesWithFilterAndOrderAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user21 = User.New("User21");
            var user22 = User.New("User22");
            var user3 = User.New("User3");
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user21);
            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user22);
            var user1HasFriendUser4 = UserLikesUserEdge.New(user1, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user21, user22, user3, user1HasFriendUser2, user1HasFriendUser3, user1HasFriendUser4)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers(
                    new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc },
                    new UserLikesUserEdgeFilter { LikesUsername = new StringFilter { BeginsWith = "User2" } })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, friends.Count);
            Assert.AreEqual("User21", friends[0].Username);
            Assert.AreEqual("User22", friends[1].Username);

            var friendsDescending = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers(
                    new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Desc },
                    new UserLikesUserEdgeFilter { LikesUsername = new StringFilter { BeginsWith = "User2" } })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, friendsDescending.Count);
            Assert.AreEqual("User22", friendsDescending[0].Username);
            Assert.AreEqual("User21", friendsDescending[1].Username);
        }

        [TestMethod]
        public async Task CanTraverseOutEdgesWithFilterAndOrderAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user21 = User.New("User21");
            var user22 = User.New("User22");
            var user3 = User.New("User3");
            var user21HasFriendUser1 = UserLikesUserEdge.New(user21, user1);
            var user22HasFriendUser1 = UserLikesUserEdge.New(user22, user1);
            var user3HasFriendUser1 = UserLikesUserEdge.New(user3, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user21, user22, user3, user21HasFriendUser1, user22HasFriendUser1, user3HasFriendUser1)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikedByUsers(
                    new UserLikesUserEdgeOrder { LikedByUsername = OrderDirection.Asc },
                    new UserLikesUserEdgeFilter { LikedByUsername = new StringFilter { BeginsWith = "User2" } })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, friends.Count);
            Assert.AreEqual("User21", friends[0].Username);
            Assert.AreEqual("User22", friends[1].Username);

            var friendsDescending = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikedByUsers(
                    new UserLikesUserEdgeOrder { LikedByUsername = OrderDirection.Desc },
                    new UserLikesUserEdgeFilter { LikedByUsername = new StringFilter { BeginsWith = "User2" } })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, friendsDescending.Count);
            Assert.AreEqual("User22", friendsDescending[0].Username);
            Assert.AreEqual("User21", friendsDescending[1].Username);
        }

        [TestMethod]
        public async Task CanTraverseInEdgesWithFilterOnlyAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user21 = User.New("User21");
            var user22 = User.New("User22");
            var user3 = User.New("User3");
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user21);
            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user22);
            var user1HasFriendUser4 = UserLikesUserEdge.New(user1, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user21, user22, user3, user1HasFriendUser2, user1HasFriendUser3, user1HasFriendUser4)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers(
                    new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc },
                    new UserLikesUserEdgeFilter { LikesUsername = new StringFilter { BeginsWith = "User2" } })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, friends.Count);
            Assert.AreEqual("User21", friends[0].Username);
            Assert.AreEqual("User22", friends[1].Username);
        }

        [TestMethod]
        public async Task CanTraverseOutEdgesWithFilterOnlyAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user21 = User.New("User21");
            var user22 = User.New("User22");
            var user3 = User.New("User3");
            var user21HasFriendUser1 = UserLikesUserEdge.New(user21, user1);
            var user22HasFriendUser1 = UserLikesUserEdge.New(user22, user1);
            var user3HasFriendUser1 = UserLikesUserEdge.New(user3, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user21, user22, user3, user21HasFriendUser1, user22HasFriendUser1, user3HasFriendUser1)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikedByUsers(
                    new UserLikesUserEdgeOrder { LikedByUsername = OrderDirection.Asc },
                    new UserLikesUserEdgeFilter { LikedByUsername = new StringFilter { BeginsWith = "User2" } })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(2, friends.Count);
            Assert.AreEqual("User21", friends[0].Username);
            Assert.AreEqual("User22", friends[1].Username);
        }

        [TestMethod]
        public async Task CanTraverseInEdgesWithOrderOnlyAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user21 = User.New("User21");
            var user22 = User.New("User22");
            var user3 = User.New("User3");
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user21);
            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user22);
            var user1HasFriendUser4 = UserLikesUserEdge.New(user1, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user21, user22, user3, user1HasFriendUser2, user1HasFriendUser3, user1HasFriendUser4)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikesUsers(new UserLikesUserEdgeOrder { LikesUsername = OrderDirection.Asc })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(3, friends.Count);
            Assert.AreEqual("User21", friends[0].Username);
            Assert.AreEqual("User22", friends[1].Username);
            Assert.AreEqual("User3", friends[2].Username);
        }

        [TestMethod]
        public async Task CanTraverseOutEdgesWithOrderOnlyAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user21 = User.New("User21");
            var user22 = User.New("User22");
            var user3 = User.New("User3");
            var user21HasFriendUser1 = UserLikesUserEdge.New(user21, user1);
            var user22HasFriendUser1 = UserLikesUserEdge.New(user22, user1);
            var user3HasFriendUser1 = UserLikesUserEdge.New(user3, user1);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user21, user22, user3, user21HasFriendUser1, user22HasFriendUser1, user3HasFriendUser1)
                .ExecuteAsync(cancellationToken);

            var friends = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Node(user1)
                .LikedByUsers(new UserLikesUserEdgeOrder { LikedByUsername = OrderDirection.Asc })
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(3, friends.Count);
            Assert.AreEqual("User21", friends[0].Username);
            Assert.AreEqual("User22", friends[1].Username);
            Assert.AreEqual("User3", friends[2].Username);
        }

        [TestMethod]
        public async Task CanUpdateEdgeWithNodeCheckForUnrelatedNodeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user2);
            var user2HasFriendUser3 = UserLikesUserEdge.New(user2, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, user3, user1HasFriendUser2, user2HasFriendUser3)
                .ExecuteAsync(cancellationToken);

            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1HasFriendUser3)
                .WithAllEdgesCheckForNodes(user1, user2, user3) // NOTE: Include user2
                .ExecuteAsync(cancellationToken);
        }

        [TestMethod]
        public async Task ThrowsUpdateEdgeWithNodeCheckForUnrelatedNodeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user1 = User.New("User1");
            var user2 = User.New("User2");
            var user3 = User.New("User3");
            var user1HasFriendUser2 = UserLikesUserEdge.New(user1, user2);
            var user2HasFriendUser3 = UserLikesUserEdge.New(user2, user3);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user1, user2, user3, user1HasFriendUser2, user2HasFriendUser3)
                .ExecuteAsync(cancellationToken);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user2.Update() with { Username = "User2 Updated" })
                .ExecuteAsync(cancellationToken);

            var user1HasFriendUser3 = UserLikesUserEdge.New(user1, user3);

            await Assert.ThrowsExceptionAsync<GraphlessDBConcurrencyException>(
                async () => await services
                    .CreateScope()
                    .GraphDB()
                    .Graph<TestGraph>()
                    .Put(user1HasFriendUser3)
                    .WithAllEdgesCheckForNodes(user1, user2, user3) // NOTE: Include user2
                    .ExecuteAsync(cancellationToken));
        }

        [TestMethod]
        public async Task CanGetVersionedNodesAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var user = User.New("Mr Smith");
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(user)
                .ExecuteAsync(cancellationToken);

            var updatedUser = user.Update() with { Username = "Mrs Smith" };
            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(updatedUser)
                .ExecuteAsync(cancellationToken);

            var userV0 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion(updatedUser, 0)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(user, userV0);

            var userV1 = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .NodeVersion(updatedUser, 1)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(updatedUser, userV1);
        }

        [TestMethod]
        public async Task CanClearAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var userCount = 7;
            var users = Enumerable
                .Range(0, userCount)
                .Select(i => User.New(i.ToString(CultureInfo.InvariantCulture)))
                .ToImmutableList();

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(users)
                .ExecuteAsync(cancellationToken);

            var returnedUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(userCount, returnedUsers.Count);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Clear()
                .ExecuteAsync(cancellationToken);

            returnedUsers = await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Users()
                .GetEntitiesAsync(true, ConnectionArguments.GetFirst(10), cancellationToken);

            Assert.AreEqual(0, returnedUsers.Count);
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
