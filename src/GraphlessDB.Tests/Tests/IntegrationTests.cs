/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

// using System;
// using System.Collections.Immutable;
// using System.Diagnostics;
// using System.Threading;
// using System.Threading.Tasks;
// using GraphlessDB;
// using Microsoft.Extensions.DependencyInjection;
// using Microsoft.VisualStudio.TestTools.UnitTesting;

// namespace GraphlessDB.Tests
// {
//     [TestClass]
//     public sealed class IntegrationTests
//     {
//         private const string TableName = "graphlessdbtest";

//         [TestMethod]
//         public async Task CanClearDownAllOldTestDataAsync()
//         {
//             // This test keeps the database test entries from growing
//             // NOTE This cannot be run concurrently with other tests
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
//             var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:u}_{Guid.NewGuid()}");
//             var queries = serviceProvider.GetRequiredService<IGraphDataQueryService>();
//             await queries.ClearAsync(new ClearRequest("TEST_", true), cancellationToken);
//         }

//         [TestMethod]
//         public async Task CanCreateUpdateAndDeleteNodeAsync()
//         {
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
//             var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:u}_{Guid.NewGuid()}");
//             var graphDB = serviceProvider.GetRequiredService<IGraphDB>();

//             var johnsmith = Person.New("johnsmith", "John", "Smith");

//             // Get
//             var cantGetPerson = await graphDB
//                 .Graph<DefaultGraph>()
//                 .NodeOrDefault<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNull(cantGetPerson);

//             // Add
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(johnsmith)
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var newPerson = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(newPerson);

//             // Update
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(newPerson.Update() with { FirstName = "John (Updated!)" })
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var updatedPerson = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.AreEqual("John (Updated!)", updatedPerson.FirstName);
//             Assert.AreEqual(newPerson.Version.NodeVersion + 1, updatedPerson.Version.NodeVersion);

//             // Delete
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(updatedPerson.Delete())
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var cantGetPersonAfterDelete = await graphDB
//                 .Graph<DefaultGraph>()
//                 .NodeOrDefault<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNull(cantGetPersonAfterDelete);
//         }

//         [TestMethod]
//         public async Task CannotUpdateStaleNodeAsync()
//         {
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
//             var cancellationToken = cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:u}_{Guid.NewGuid()}");
//             var graphDB = serviceProvider.GetRequiredService<IGraphDB>();

//             var johnsmith = Person.New("johnsmith", "John", "Smith");

//             // Add
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(johnsmith)
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var newPerson = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(newPerson);

//             // Update
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(newPerson.Update() with { FirstName = "John (Updated!)" })
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var updatedPerson = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.AreEqual("John (Updated!)", updatedPerson.FirstName);
//             Assert.AreEqual(newPerson.Version.NodeVersion + 1, updatedPerson.Version.NodeVersion);

//             // Attempt update on stale version
//             await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
//             {
//                 await graphDB
//                     .Graph<DefaultGraph>()
//                     .Put(newPerson.Update() with { FirstName = "John (Attempted update!)" })
//                     .ExecuteAsync(cancellationToken);
//             });
//         }

//         [TestMethod]
//         public async Task MultiNodeCreateAsync()
//         {
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
//             var cancellationToken = cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:u}_{Guid.NewGuid()}");
//             var graphDB = serviceProvider.GetRequiredService<IGraphDB>();

//             var johnsmith = Person.New("johnsmith", "John", "Smith");
//             var janesmith = Person.New("janesmith", "Jane", "Smith");

//             // Add
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(ImmutableList.Create<IEntity>(
//                     johnsmith,
//                     janesmith))
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var newPerson1 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(newPerson1);

//             var newPerson2 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(janesmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(newPerson2);

//             // Attempted create
//             await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
//             {
//                 await graphDB
//                     .Graph<DefaultGraph>()
//                     .Put(johnsmith)
//                     .ExecuteAsync(cancellationToken);
//             });
//         }

//         // ThrowsOnMultiNodeUpdateWithExistingNodeAsync

//         // ThrowsOnMultiNodeUpdateWithStaleNodeAsync

//         // ThrowsOnMultiNodeUpdateWithMissingNodeAsync

//         [TestMethod]
//         public async Task MultiRelatedNodeUpdateAsync()
//         {
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(300000));
//             var cancellationToken = cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:u}_{Guid.NewGuid()}");
//             var graphDB = serviceProvider.GetRequiredService<IGraphDB>();

//             var johnsmith = Person.New("johnsmith", "John", "Smith");
//             var janesmith = Person.New("janesmith", "Jane", "Smith");

//             // Add
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(ImmutableList.Create<IEntity>(
//                     johnsmith,
//                     janesmith))
//                 .ExecuteAsync(cancellationToken);

//             // Get
//             var newPerson1 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(newPerson1);

//             var newPerson2 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(janesmith.Id)
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(newPerson2);

//             // Relate
//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(HasFriendEdge.New(johnsmith.Id, janesmith.Id))
//                 .WithAllEdgesCheckForNodes(johnsmith, janesmith)
//                 .ExecuteAsync(cancellationToken);

//             // Traverse edge
//             var friendOfPerson1 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .InToEdges<HasFriendEdge, Person>()
//                 .OutFromEdges()
//                 .Single()
//                 .Node()
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(friendOfPerson1);
//             Assert.AreEqual("janesmith", friendOfPerson1.UserName);

//             // Traverse edge in reverse
//             var friendOfPerson2 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(janesmith.Id)
//                 .OutToEdges<HasFriendEdge, Person>()
//                 .InFromEdges()
//                 .Single()
//                 .Node()
//                 .GetAsync(cancellationToken);

//             Assert.IsNotNull(friendOfPerson2);
//             Assert.AreEqual("johnsmith", friendOfPerson2.UserName);

//             // Delete edge
//             var hasFriendEdge = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(janesmith.Id)
//                 .OutToEdges<HasFriendEdge, Person>()
//                 .Single()
//                 .Edge()
//                 .GetAsync(cancellationToken);

//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(hasFriendEdge.Delete())
//                 .WithNoEdgeChecksForAllNodes()
//                 .ExecuteAsync(cancellationToken);

//             // Attempt traverse edge
//             var noFriendOfPerson1 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(johnsmith.Id)
//                 .InToEdges<HasFriendEdge, Person>()
//                 .OutFromEdges()
//                 .SingleOrDefault()
//                 .Node()
//                 .GetAsync(cancellationToken);

//             Assert.IsNull(noFriendOfPerson1);

//             // Attempt traverse edge in reverse
//             var noFriendOfPerson2 = await graphDB
//                 .Graph<DefaultGraph>()
//                 .Node<Person>(janesmith.Id)
//                 .OutToEdges<HasFriendEdge, Person>()
//                 .InFromEdges()
//                 .SingleOrDefault()
//                 .Node()
//                 .GetAsync(cancellationToken);

//             Assert.IsNull(noFriendOfPerson2);

//             // Attempted create
//             await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
//             {
//                 await graphDB
//                     .Graph<DefaultGraph>()
//                     .Put(johnsmith)
//                     .ExecuteAsync(cancellationToken);
//             });
//         }

//         // CanGetVersionHistory
//     }
// }
