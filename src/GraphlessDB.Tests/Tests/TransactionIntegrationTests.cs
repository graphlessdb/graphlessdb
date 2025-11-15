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
// using System.Linq;
// using System.Threading;
// using System.Threading.Tasks;
// using Microsoft.Extensions.DependencyInjection;
// using Microsoft.VisualStudio.TestTools.UnitTesting;

// namespace GraphlessDB.Tests
// {
//     [TestClass]
//     public sealed class TransactionIntegrationTests
//     {
//         private const string TableName = "graphlessdbtest";

//         [TestMethod]
//         public async Task CanCreateManyPeopleAsync()
//         {
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
//             var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid().ToString()[..4]}");
//             var graphDB = serviceProvider.GetRequiredService<IGraphDB>();
//             var mutId = MutationId.Create(Guid.NewGuid().ToString());

//             var people = Enumerable
//                 .Range(0, 10)
//                 .Select(i => Person.New($"LockPerson_{i}", "LockPerson", $"{i}"))
//                 .ToImmutableList();

//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(people)
//                 .ExecuteAsync(mutId, cancellationToken);

//             var refetchedPeople = await Task.WhenAll(people
//                 .Select(person => graphDB
//                     .Graph<DefaultGraph>()
//                     .Node<Person>(person.Id)
//                     .GetAsync(cancellationToken)));

//             Assert.IsTrue(refetchedPeople.All(v => v != null));
//         }


//         [TestMethod]
//         public async Task CanLockEdgesAsync()
//         {
//             var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
//             var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
//             var serviceProvider = ServiceProviderTestHelper.GetServiceProvider(TableName, $"TEST_{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid().ToString()[..4]}");
//             var graphDB = serviceProvider.GetRequiredService<IGraphDB>();
//             var mutId = MutationId.Create(Guid.NewGuid().ToString());

//             var person1 = Person.New($"Person_1", "Person", "1");
//             var person2 = Person.New($"Person_2", "Person", "2");
//             var person3 = Person.New($"Person_3", "Person", "3");

//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(person1, person2, person3)
//                 .ExecuteAsync(mutId, cancellationToken);

//             var hasFriend12 = HasFriendEdge
//                 .New(person1.Id, person2.Id);

//             var hasFriend23 = HasFriendEdge
//                 .New(person2.Id, person3.Id);

//             await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(hasFriend12)
//                 .WithAllEdgesCheckForNodes(person1, person2)
//                 .ExecuteAsync(mutId, cancellationToken);

//             await Assert.ThrowsExceptionAsync<GraphConcurrencyException>(async () => await graphDB
//                 .Graph<DefaultGraph>()
//                 .Put(hasFriend23)
//                 .WithAllEdgesCheckForNodes(person2, person3)
//                 .ExecuteAsync(mutId, cancellationToken));
//         }
//     }
// }
