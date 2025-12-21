/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

#pragma warning disable CA1707 // Identifiers should not contain underscores - test methods use category prefixes for clarity

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
    /// <summary>
    /// Advanced Datalog tests covering common real-world use cases:
    /// 1. Social Network Analysis - Friend recommendations, influence paths
    /// 2. Access Control - Role-based permissions, privilege escalation detection
    /// 3. Knowledge Graphs - Ontology reasoning, type hierarchies
    /// 4. Supply Chain - Bill of materials, dependency tracking
    /// 5. Genealogy - Family trees, ancestor queries
    /// 6. Network Routing - Shortest paths, reachability
    /// </summary>
    [TestClass]
    public abstract class AdvancedDatalogQueryTests
    {
        protected abstract IServiceCollection ConfigureGraphDBServices(IServiceCollection services);

        #region Social Network Analysis

        [TestMethod]
        public async Task SocialNetwork_FriendOfFriendRecommendationsAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Create social network: alice -> bob -> charlie, alice -> david
            var alice = Person.New("alice", 30);
            var bob = Person.New("bob", 28);
            var charlie = Person.New("charlie", 32);
            var david = Person.New("david", 29);
            var aliceFriendBob = FriendshipEdge.New(alice, bob);
            var bobFriendCharlie = FriendshipEdge.New(bob, charlie);
            var aliceFriendDavid = FriendshipEdge.New(alice, david);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(alice, bob, charlie, david, aliceFriendBob, bobFriendCharlie, aliceFriendDavid))
                .ExecuteAsync(cancellationToken);

            // Find friends of friends (2 hops away) for alice - potential friend recommendations
            var results = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var user)
                .Var<Person>(out var recommendation)
                .Match(user, m => m.WhereEquals("Name", "alice"))
                .Recursive<FriendshipEdge, Person>(user, recommendation, r => r
                    .Via(e => e)
                    .MinDepth(2)
                    .MaxDepth(2)
                    .BreadthFirst())
                .Select(recommendation)
                .GetAsync(true, cancellationToken);

            // Should find charlie (friend of bob, who is friend of alice)
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("charlie", results[0].Name);
        }

        [TestMethod]
        public async Task SocialNetwork_MutualFriendsAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Network: alice -> bob, alice -> charlie, david -> bob, david -> charlie
            var alice = Person.New("alice", 30);
            var bob = Person.New("bob", 28);
            var charlie = Person.New("charlie", 32);
            var david = Person.New("david", 29);
            var aliceFriendBob = FriendshipEdge.New(alice, bob);
            var aliceFriendCharlie = FriendshipEdge.New(alice, charlie);
            var davidFriendBob = FriendshipEdge.New(david, bob);
            var davidFriendCharlie = FriendshipEdge.New(david, charlie);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(alice, bob, charlie, david, aliceFriendBob, aliceFriendCharlie, davidFriendBob, davidFriendCharlie))
                .ExecuteAsync(cancellationToken);

            // Find mutual friends between alice and david
            var aliceFriends = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var alice2)
                .Var<Person>(out var aliceFriend)
                .Match(alice2, m => m.WhereEquals("Name", "alice"))
                .Edge<FriendshipEdge, Person, Person>(alice2, aliceFriend)
                .Select(aliceFriend)
                .GetAsync(true, cancellationToken);

            var davidFriends = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var david2)
                .Var<Person>(out var davidFriend)
                .Match(david2, m => m.WhereEquals("Name", "david"))
                .Edge<FriendshipEdge, Person, Person>(david2, davidFriend)
                .Select(davidFriend)
                .GetAsync(true, cancellationToken);

            var mutualFriends = aliceFriends.Select(f => f.Name).Intersect(davidFriends.Select(f => f.Name)).ToList();
            Assert.AreEqual(2, mutualFriends.Count);
            Assert.IsTrue(mutualFriends.Contains("bob"));
            Assert.IsTrue(mutualFriends.Contains("charlie"));
        }

        #endregion

        #region Access Control & Security

        [TestMethod]
        public async Task AccessControl_TransitiveRoleInheritanceAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Role hierarchy: Admin inherits Manager, Manager inherits User
            var adminRole = Role.New("Admin");
            var managerRole = Role.New("Manager");
            var userRole = Role.New("User");
            var adminInheritsManager = RoleInheritsRoleEdge.New(adminRole, managerRole);
            var managerInheritsUser = RoleInheritsRoleEdge.New(managerRole, userRole);

            // Permissions
            var readPerm = PermissionNode.New("Read");
            var writePerm = PermissionNode.New("Write");
            var deletePerm = PermissionNode.New("Delete");
            var userHasRead = RoleHasPermissionEdge.New(userRole, readPerm);
            var managerHasWrite = RoleHasPermissionEdge.New(managerRole, writePerm);
            var adminHasDelete = RoleHasPermissionEdge.New(adminRole, deletePerm);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(
                    adminRole, managerRole, userRole,
                    readPerm, writePerm, deletePerm,
                    adminInheritsManager, managerInheritsUser,
                    userHasRead, managerHasWrite, adminHasDelete))
                .ExecuteAsync(cancellationToken);

            // Find all permissions available to Admin role (direct + inherited)
            var directPerms = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Role>(out var role)
                .Var<PermissionNode>(out var permission)
                .Match(role, m => m.WhereEquals("Name", "Admin"))
                .Edge<RoleHasPermissionEdge, Role, PermissionNode>(role, permission)
                .Select(permission)
                .GetAsync(true, cancellationToken);

            var inheritedRoles = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Role>(out var adminRole2)
                .Var<Role>(out var inheritedRole)
                .Match(adminRole2, m => m.WhereEquals("Name", "Admin"))
                .Recursive<RoleInheritsRoleEdge, Role>(adminRole2, inheritedRole, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(inheritedRole)
                .GetAsync(true, cancellationToken);

            // Get permissions from inherited roles
            var allPermissions = directPerms.ToList();
            foreach (var inherRole in inheritedRoles)
            {
                var rolePerms = await services
                    .CreateScope()
                    .GraphDB()
                    .Datalog<TestGraph>()
                    .Var<Role>(out var r)
                    .Var<PermissionNode>(out var p)
                    .Match(r, m => m.WhereEquals("Name", inherRole.Name))
                    .Edge<RoleHasPermissionEdge, Role, PermissionNode>(r, p)
                    .Select(p)
                    .GetAsync(true, cancellationToken);
                allPermissions.AddRange(rolePerms);
            }

            var uniquePermissions = allPermissions.Select(p => p.Name).Distinct().ToList();
            Assert.AreEqual(3, uniquePermissions.Count);
            Assert.IsTrue(uniquePermissions.Contains("Read"));
            Assert.IsTrue(uniquePermissions.Contains("Write"));
            Assert.IsTrue(uniquePermissions.Contains("Delete"));
        }

        [TestMethod]
        public async Task AccessControl_DetectPrivilegeEscalationPathAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Security model: user can delegate to another user (potential escalation)
            var lowUser = Person.New("lowPrivUser", 25);
            var midUser = Person.New("midPrivUser", 30);
            var highUser = Person.New("highPrivUser", 35);
            var lowDelegatesMid = DelegatesEdge.New(lowUser, midUser);
            var midDelegatesHigh = DelegatesEdge.New(midUser, highUser);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(lowUser, midUser, highUser, lowDelegatesMid, midDelegatesHigh))
                .ExecuteAsync(cancellationToken);

            // Detect if low privilege user can reach high privilege user through delegation chain
            var reachableUsers = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var start)
                .Var<Person>(out var reachable)
                .Match(start, m => m.WhereEquals("Name", "lowPrivUser"))
                .Recursive<DelegatesEdge, Person>(start, reachable, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(5)
                    .BreadthFirst())
                .Select(reachable)
                .GetAsync(true, cancellationToken);

            var canReachHighPriv = reachableUsers.Any(u => u.Name == "highPrivUser");
            Assert.IsTrue(canReachHighPriv, "Privilege escalation path detected");
        }

        #endregion

        #region Knowledge Graphs & Ontologies

        [TestMethod]
        public async Task KnowledgeGraph_TypeHierarchyReasoningAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Type hierarchy: Mammal -> Animal, Dog -> Mammal, Labrador -> Dog
            var animal = Concept.New("Animal");
            var mammal = Concept.New("Mammal");
            var dog = Concept.New("Dog");
            var labrador = Concept.New("Labrador");
            var mammalIsAnimal = IsAEdge.New(mammal, animal);
            var dogIsMammal = IsAEdge.New(dog, mammal);
            var labradorIsDog = IsAEdge.New(labrador, dog);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(animal, mammal, dog, labrador, mammalIsAnimal, dogIsMammal, labradorIsDog))
                .ExecuteAsync(cancellationToken);

            // Query: What are all the supertypes of Labrador?
            var supertypes = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Concept>(out var specificType)
                .Var<Concept>(out var supertype)
                .Match(specificType, m => m.WhereEquals("Name", "Labrador"))
                .Recursive<IsAEdge, Concept>(specificType, supertype, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(supertype)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(3, supertypes.Count);
            var typeNames = supertypes.Select(t => t.Name).ToList();
            Assert.IsTrue(typeNames.Contains("Dog"));
            Assert.IsTrue(typeNames.Contains("Mammal"));
            Assert.IsTrue(typeNames.Contains("Animal"));
        }

        [TestMethod]
        public async Task KnowledgeGraph_PropertyInheritanceAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Concepts with properties
            var animal = Concept.New("Animal");
            var dog = Concept.New("Dog");
            var breathes = PropertyNode.New("Breathes");
            var hasFur = PropertyNode.New("HasFur");
            var dogIsAnimal = IsAEdge.New(dog, animal);
            var animalBreathes = ConceptHasPropertyEdge.New(animal, breathes);
            var dogHasFur = ConceptHasPropertyEdge.New(dog, hasFur);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(animal, dog, breathes, hasFur, dogIsAnimal, animalBreathes, dogHasFur))
                .ExecuteAsync(cancellationToken);

            // Find all properties of Dog (direct + inherited)
            var directProps = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Concept>(out var concept)
                .Var<PropertyNode>(out var prop)
                .Match(concept, m => m.WhereEquals("Name", "Dog"))
                .Edge<ConceptHasPropertyEdge, Concept, PropertyNode>(concept, prop)
                .Select(prop)
                .GetAsync(true, cancellationToken);

            var supertypes = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Concept>(out var dog2)
                .Var<Concept>(out var supertype)
                .Match(dog2, m => m.WhereEquals("Name", "Dog"))
                .Recursive<IsAEdge, Concept>(dog2, supertype, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(supertype)
                .GetAsync(true, cancellationToken);

            var allProps = directProps.ToList();
            foreach (var super in supertypes)
            {
                var superProps = await services
                    .CreateScope()
                    .GraphDB()
                    .Datalog<TestGraph>()
                    .Var<Concept>(out var c)
                    .Var<PropertyNode>(out var p)
                    .Match(c, m => m.WhereEquals("Name", super.Name))
                    .Edge<ConceptHasPropertyEdge, Concept, PropertyNode>(c, p)
                    .Select(p)
                    .GetAsync(true, cancellationToken);
                allProps.AddRange(superProps);
            }

            var uniqueProps = allProps.Select(p => p.Name).Distinct().ToList();
            Assert.AreEqual(2, uniqueProps.Count);
            Assert.IsTrue(uniqueProps.Contains("HasFur"));
            Assert.IsTrue(uniqueProps.Contains("Breathes"));
        }

        #endregion

        #region Supply Chain & Bill of Materials

        [TestMethod]
        public async Task SupplyChain_BillOfMaterialsExplosionAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Product hierarchy: Bicycle -> (Frame, Wheels), Frame -> (Steel, Welds)
            var bicycle = Product.New("Bicycle");
            var frame = Product.New("Frame");
            var wheels = Product.New("Wheels");
            var steel = Product.New("Steel");
            var welds = Product.New("Welds");
            var bicycleHasFrame = ComponentOfEdge.New(frame, bicycle, 1);
            var bicycleHasWheels = ComponentOfEdge.New(wheels, bicycle, 2);
            var frameHasSteel = ComponentOfEdge.New(steel, frame, 5);
            var frameHasWelds = ComponentOfEdge.New(welds, frame, 10);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(bicycle, frame, wheels, steel, welds, bicycleHasFrame, bicycleHasWheels, frameHasSteel, frameHasWelds))
                .ExecuteAsync(cancellationToken);

            // Find all components needed to build a bicycle (BOM explosion)
            var components = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Product>(out var finalProduct)
                .Var<Product>(out var component)
                .Match(finalProduct, m => m.WhereEquals("Name", "Bicycle"))
                .Recursive<ComponentOfEdge, Product>(component, finalProduct, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(component)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(4, components.Count);
            var componentNames = components.Select(c => c.Name).ToList();
            Assert.IsTrue(componentNames.Contains("Frame"));
            Assert.IsTrue(componentNames.Contains("Wheels"));
            Assert.IsTrue(componentNames.Contains("Steel"));
            Assert.IsTrue(componentNames.Contains("Welds"));
        }

        [TestMethod]
        public async Task SupplyChain_WhereUsedQueryAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Steel is used in multiple products
            var bicycle = Product.New("Bicycle");
            var car = Product.New("Car");
            var frame = Product.New("Frame");
            var chassis = Product.New("Chassis");
            var steel = Product.New("Steel");
            var bicycleHasFrame = ComponentOfEdge.New(frame, bicycle, 1);
            var carHasChassis = ComponentOfEdge.New(chassis, car, 1);
            var frameHasSteel = ComponentOfEdge.New(steel, frame, 5);
            var chassisHasSteel = ComponentOfEdge.New(steel, chassis, 20);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(bicycle, car, frame, chassis, steel, bicycleHasFrame, carHasChassis, frameHasSteel, chassisHasSteel))
                .ExecuteAsync(cancellationToken);

            // Where-used query: Find all products that use steel
            var usingSteelDirectly = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Product>(out var component)
                .Var<Product>(out var parent)
                .Match(component, m => m.WhereEquals("Name", "Steel"))
                .Edge<ComponentOfEdge, Product, Product>(component, parent)
                .Select(parent)
                .GetAsync(true, cancellationToken);

            // Find top-level products
            var topLevelProducts = usingSteelDirectly.ToList();
            foreach (var intermediate in usingSteelDirectly)
            {
                var topLevel = await services
                    .CreateScope()
                    .GraphDB()
                    .Datalog<TestGraph>()
                    .Var<Product>(out var inter)
                    .Var<Product>(out var top)
                    .Match(inter, m => m.WhereEquals("Name", intermediate.Name))
                    .Recursive<ComponentOfEdge, Product>(inter, top, r => r
                        .Via(e => e)
                        .MinDepth(1)
                        .MaxDepth(10)
                        .BreadthFirst())
                    .Select(top)
                    .GetAsync(true, cancellationToken);
                topLevelProducts.AddRange(topLevel);
            }

            var uniqueProducts = topLevelProducts.Select(p => p.Name).Distinct().ToList();
            Assert.IsTrue(uniqueProducts.Contains("Bicycle") || uniqueProducts.Contains("Frame"));
            Assert.IsTrue(uniqueProducts.Contains("Car") || uniqueProducts.Contains("Chassis"));
        }

        #endregion

        #region Genealogy & Family Trees

        [TestMethod]
        public async Task Genealogy_FindAllAncestorsAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Family tree: John -> (Father: Bob, Mother: Alice), Bob -> Father: Charlie
            var john = Person.New("John", 30);
            var bob = Person.New("Bob", 55);
            var alice = Person.New("Alice", 53);
            var charlie = Person.New("Charlie", 80);
            var johnParentBob = ParentOfEdge.New(bob, john);
            var johnParentAlice = ParentOfEdge.New(alice, john);
            var bobParentCharlie = ParentOfEdge.New(charlie, bob);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(john, bob, alice, charlie, johnParentBob, johnParentAlice, bobParentCharlie))
                .ExecuteAsync(cancellationToken);

            // Find all ancestors of John
            var ancestors = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var descendant)
                .Var<Person>(out var ancestor)
                .Match(descendant, m => m.WhereEquals("Name", "John"))
                .Recursive<ParentOfEdge, Person>(ancestor, descendant, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(ancestor)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(3, ancestors.Count);
            var ancestorNames = ancestors.Select(a => a.Name).ToList();
            Assert.IsTrue(ancestorNames.Contains("Bob"));
            Assert.IsTrue(ancestorNames.Contains("Alice"));
            Assert.IsTrue(ancestorNames.Contains("Charlie"));
        }

        [TestMethod]
        public async Task Genealogy_FindCommonAncestorAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Family tree: Charlie -> Bob, Charlie -> Carol, Bob -> John, Carol -> Jane
            var john = Person.New("John", 30);
            var jane = Person.New("Jane", 28);
            var bob = Person.New("Bob", 55);
            var carol = Person.New("Carol", 53);
            var charlie = Person.New("Charlie", 80);
            var bobParentJohn = ParentOfEdge.New(bob, john);
            var carolParentJane = ParentOfEdge.New(carol, jane);
            var charlieParentBob = ParentOfEdge.New(charlie, bob);
            var charlieParentCarol = ParentOfEdge.New(charlie, carol);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(john, jane, bob, carol, charlie, bobParentJohn, carolParentJane, charlieParentBob, charlieParentCarol))
                .ExecuteAsync(cancellationToken);

            // Find common ancestors of John and Jane
            var johnAncestors = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var john2)
                .Var<Person>(out var johnAncestor)
                .Match(john2, m => m.WhereEquals("Name", "John"))
                .Recursive<ParentOfEdge, Person>(johnAncestor, john2, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(johnAncestor)
                .GetAsync(true, cancellationToken);

            var janeAncestors = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var jane2)
                .Var<Person>(out var janeAncestor)
                .Match(jane2, m => m.WhereEquals("Name", "Jane"))
                .Recursive<ParentOfEdge, Person>(janeAncestor, jane2, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(janeAncestor)
                .GetAsync(true, cancellationToken);

            var commonAncestors = johnAncestors.Select(a => a.Name).Intersect(janeAncestors.Select(a => a.Name)).ToList();
            Assert.AreEqual(1, commonAncestors.Count);
            Assert.IsTrue(commonAncestors.Contains("Charlie"));
        }

        #endregion

        #region Network Routing & Reachability

        [TestMethod]
        public async Task Network_ReachabilityAnalysisAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Network topology: ServerA -> ServerB -> ServerC, ServerA -> ServerD (ServerC isolated from D)
            var serverA = Server.New("ServerA");
            var serverB = Server.New("ServerB");
            var serverC = Server.New("ServerC");
            var serverD = Server.New("ServerD");
            var linkAB = NetworkLinkEdge.New(serverA, serverB);
            var linkBC = NetworkLinkEdge.New(serverB, serverC);
            var linkAD = NetworkLinkEdge.New(serverA, serverD);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(serverA, serverB, serverC, serverD, linkAB, linkBC, linkAD))
                .ExecuteAsync(cancellationToken);

            // Check reachability from ServerA
            var reachableFromA = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Server>(out var source)
                .Var<Server>(out var destination)
                .Match(source, m => m.WhereEquals("Name", "ServerA"))
                .Recursive<NetworkLinkEdge, Server>(source, destination, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(destination)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(3, reachableFromA.Count);
            var reachableNames = reachableFromA.Select(s => s.Name).ToList();
            Assert.IsTrue(reachableNames.Contains("ServerB"));
            Assert.IsTrue(reachableNames.Contains("ServerC"));
            Assert.IsTrue(reachableNames.Contains("ServerD"));
        }

        [TestMethod]
        public async Task Network_IsolatedComponentDetectionAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Two separate networks: A-B-C and D-E
            var serverA = Server.New("ServerA");
            var serverB = Server.New("ServerB");
            var serverC = Server.New("ServerC");
            var serverD = Server.New("ServerD");
            var serverE = Server.New("ServerE");
            var linkAB = NetworkLinkEdge.New(serverA, serverB);
            var linkBC = NetworkLinkEdge.New(serverB, serverC);
            var linkDE = NetworkLinkEdge.New(serverD, serverE);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(serverA, serverB, serverC, serverD, serverE, linkAB, linkBC, linkDE))
                .ExecuteAsync(cancellationToken);

            // Check if ServerD is reachable from ServerA
            var reachableFromA = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Server>(out var source)
                .Var<Server>(out var destination)
                .Match(source, m => m.WhereEquals("Name", "ServerA"))
                .Recursive<NetworkLinkEdge, Server>(source, destination, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(destination)
                .GetAsync(true, cancellationToken);

            var canReachD = reachableFromA.Any(s => s.Name == "ServerD");
            Assert.IsFalse(canReachD, "ServerD should be isolated from ServerA");

            var reachableFromD = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Server>(out var sourceD)
                .Var<Server>(out var destFromD)
                .Match(sourceD, m => m.WhereEquals("Name", "ServerD"))
                .Recursive<NetworkLinkEdge, Server>(sourceD, destFromD, r => r
                    .Via(e => e)
                    .MinDepth(1)
                    .MaxDepth(10)
                    .BreadthFirst())
                .Select(destFromD)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(1, reachableFromD.Count);
            Assert.AreEqual("ServerE", reachableFromD[0].Name);
        }

        [TestMethod]
        public async Task Network_MultiHopPathWithConstraintsAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Network path: A -> B -> C -> D -> E
            var serverA = Server.New("ServerA");
            var serverB = Server.New("ServerB");
            var serverC = Server.New("ServerC");
            var serverD = Server.New("ServerD");
            var serverE = Server.New("ServerE");
            var linkAB = NetworkLinkEdge.New(serverA, serverB);
            var linkBC = NetworkLinkEdge.New(serverB, serverC);
            var linkCD = NetworkLinkEdge.New(serverC, serverD);
            var linkDE = NetworkLinkEdge.New(serverD, serverE);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(serverA, serverB, serverC, serverD, serverE, linkAB, linkBC, linkCD, linkDE))
                .ExecuteAsync(cancellationToken);

            // Find servers exactly 3 hops away from ServerA
            var threeHopsAway = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Server>(out var source)
                .Var<Server>(out var destination)
                .Match(source, m => m.WhereEquals("Name", "ServerA"))
                .Recursive<NetworkLinkEdge, Server>(source, destination, r => r
                    .Via(e => e)
                    .MinDepth(3)
                    .MaxDepth(3)
                    .BreadthFirst())
                .Select(destination)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(1, threeHopsAway.Count);
            Assert.AreEqual("ServerD", threeHopsAway[0].Name);
        }

        #endregion

        #region Complex Multi-Pattern Queries

        [TestMethod]
        public async Task Complex_MultiEdgeTypeTraversalAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Person works at Company, Company produces Product
            var alice = Person.New("Alice", 30);
            var techCorp = Company.New("TechCorp");
            var software = Product.New("Software");
            var aliceWorksAtTech = WorksAtEdge.New(alice, techCorp);
            var techProducesSoftware = ProducesEdge.New(techCorp, software);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(alice, techCorp, software, aliceWorksAtTech, techProducesSoftware))
                .ExecuteAsync(cancellationToken);

            // Find products made by companies where Alice works
            var aliceCompanies = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var person)
                .Var<Company>(out var company)
                .Match(person, m => m.WhereEquals("Name", "Alice"))
                .Edge<WorksAtEdge, Person, Company>(person, company)
                .Select(company)
                .GetAsync(true, cancellationToken);

            var products = new System.Collections.Generic.List<Product>();
            foreach (var comp in aliceCompanies)
            {
                var compProducts = await services
                    .CreateScope()
                    .GraphDB()
                    .Datalog<TestGraph>()
                    .Var<Company>(out var c)
                    .Var<Product>(out var p)
                    .Match(c, m => m.WhereEquals("Name", comp.Name))
                    .Edge<ProducesEdge, Company, Product>(c, p)
                    .Select(p)
                    .GetAsync(true, cancellationToken);
                products.AddRange(compProducts);
            }

            Assert.AreEqual(1, products.Count);
            Assert.AreEqual("Software", products[0].Name);
        }

        [TestMethod]
        public async Task Complex_DiamondPatternAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            // Diamond: A -> B, A -> C, B -> D, C -> D
            var personA = Person.New("PersonA", 30);
            var personB = Person.New("PersonB", 28);
            var personC = Person.New("PersonC", 32);
            var personD = Person.New("PersonD", 35);
            var edgeAB = FriendshipEdge.New(personA, personB);
            var edgeAC = FriendshipEdge.New(personA, personC);
            var edgeBD = FriendshipEdge.New(personB, personD);
            var edgeCD = FriendshipEdge.New(personC, personD);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(personA, personB, personC, personD, edgeAB, edgeAC, edgeBD, edgeCD))
                .ExecuteAsync(cancellationToken);

            // Find all paths from A with depth 2 (should find D through two paths)
            var twoHopsAway = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var start)
                .Var<Person>(out var end)
                .Match(start, m => m.WhereEquals("Name", "PersonA"))
                .Recursive<FriendshipEdge, Person>(start, end, r => r
                    .Via(e => e)
                    .MinDepth(2)
                    .MaxDepth(2)
                    .BreadthFirst())
                .Select(end)
                .GetAsync(true, cancellationToken);

            // Should find D (reachable via B and C)
            Assert.IsTrue(twoHopsAway.Any(p => p.Name == "PersonD"));
        }

        #endregion

        #region Age-Based Filtering

        [TestMethod]
        public async Task Filtering_FindPeopleByAgeRangeAsync()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;
            var services = GetServiceProvider();

            var person1 = Person.New("Person1", 25);
            var person2 = Person.New("Person2", 30);
            var person3 = Person.New("Person3", 35);
            var person4 = Person.New("Person4", 40);

            await services
                .CreateScope()
                .GraphDB()
                .Graph<TestGraph>()
                .Put(ImmutableList.Create<IEntity>(person1, person2, person3, person4))
                .ExecuteAsync(cancellationToken);

            // Find people aged 30 (exact match using WhereEquals)
            var age30 = await services
                .CreateScope()
                .GraphDB()
                .Datalog<TestGraph>()
                .Var<Person>(out var person)
                .Match(person, m => m.WhereEquals("Age", "30"))
                .Select(person)
                .GetAsync(true, cancellationToken);

            Assert.AreEqual(1, age30.Count);
            Assert.AreEqual("Person2", age30[0].Name);
        }

        #endregion

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
                    // Original types
                    o.TypeMappings.Add(nameof(Car), typeof(Car));
                    o.TypeMappings.Add(nameof(Manufacturer), typeof(Manufacturer));
                    o.TypeMappings.Add(nameof(ManufacturerMakesCarEdge), typeof(ManufacturerMakesCarEdge));
                    o.TypeMappings.Add(nameof(User), typeof(User));
                    o.TypeMappings.Add(nameof(UserLikesUserEdge), typeof(UserLikesUserEdge));
                    o.TypeMappings.Add(nameof(UserOwnsCarEdge), typeof(UserOwnsCarEdge));

                    // New types for advanced tests
                    o.TypeMappings.Add(nameof(Person), typeof(Person));
                    o.TypeMappings.Add(nameof(Role), typeof(Role));
                    o.TypeMappings.Add(nameof(PermissionNode), typeof(PermissionNode));
                    o.TypeMappings.Add(nameof(Concept), typeof(Concept));
                    o.TypeMappings.Add(nameof(PropertyNode), typeof(PropertyNode));
                    o.TypeMappings.Add(nameof(Product), typeof(Product));
                    o.TypeMappings.Add(nameof(Server), typeof(Server));
                    o.TypeMappings.Add(nameof(Company), typeof(Company));

                    // New edges
                    o.TypeMappings.Add(nameof(FriendshipEdge), typeof(FriendshipEdge));
                    o.TypeMappings.Add(nameof(DelegatesEdge), typeof(DelegatesEdge));
                    o.TypeMappings.Add(nameof(RoleInheritsRoleEdge), typeof(RoleInheritsRoleEdge));
                    o.TypeMappings.Add(nameof(RoleHasPermissionEdge), typeof(RoleHasPermissionEdge));
                    o.TypeMappings.Add(nameof(IsAEdge), typeof(IsAEdge));
                    o.TypeMappings.Add(nameof(ConceptHasPropertyEdge), typeof(ConceptHasPropertyEdge));
                    o.TypeMappings.Add(nameof(ComponentOfEdge), typeof(ComponentOfEdge));
                    o.TypeMappings.Add(nameof(ParentOfEdge), typeof(ParentOfEdge));
                    o.TypeMappings.Add(nameof(NetworkLinkEdge), typeof(NetworkLinkEdge));
                    o.TypeMappings.Add(nameof(WorksAtEdge), typeof(WorksAtEdge));
                    o.TypeMappings.Add(nameof(ProducesEdge), typeof(ProducesEdge));
                })
                .AddGraphlessDBEntitySerializerOptions(o =>
                {
                    o.JsonContext = AdvancedGraphlessDBTestContext.Default;
                });

            services.AddTestInstrumentation(Debugger.IsAttached);

            ConfigureGraphDBServices(services);

            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
        }
    }
}
