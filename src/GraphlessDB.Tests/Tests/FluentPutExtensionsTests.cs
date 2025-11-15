/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Collections;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class FluentPutExtensionsTests
    {
        private sealed class MockGraphQueryExecutionService : IGraphQueryExecutionService
        {
            public PutRequest? LastPutRequest { get; private set; }
            public int PutAsyncCallCount { get; private set; }

            public Task ClearAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GraphExecutionContext> GetAsync(ImmutableTree<string, GraphQueryNode> query, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task MutateAsync(Func<Task> operation, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<T> MutateAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
            {
                LastPutRequest = request;
                PutAsyncCallCount++;
                return Task.CompletedTask;
            }
        }

        private static User CreateTestUser(string username)
        {
            return User.New(username);
        }

        private static FluentPut CreateFluentPut(MockGraphQueryExecutionService service)
        {
            return new FluentPut(service, PutQuery.Empty);
        }

        private static FluentPut CreateFluentPutWithEntities(MockGraphQueryExecutionService service, params IEntity[] entities)
        {
            return new FluentPut(service, new PutQuery(
                entities.ToImmutableList(),
                ImmutableList<INode>.Empty,
                ImmutableList<EdgeByPropCheck>.Empty,
                ImmutableList<string>.Empty,
                false));
        }

        #region ExecuteAsync Tests

        [TestMethod]
        public async Task ExecuteAsyncWithoutMutationIdCallsPutAsync()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("testuser");
            var fluentPut = CreateFluentPutWithEntities(service, user);
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.AreEqual(1, service.PutAsyncCallCount);
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(1, service.LastPutRequest.PutEntities.Count);
            Assert.AreEqual(user, service.LastPutRequest.PutEntities[0]);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithoutMutationIdCreatesMutationId()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = CreateFluentPut(service);
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.IsNotNull(service.LastPutRequest.MutationId);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithMutationIdCallsPutAsync()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("testuser");
            var fluentPut = CreateFluentPutWithEntities(service, user);
            var mutationId = MutationId.Create("test-mutation");
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(mutationId, cancellationToken);

            // Assert
            Assert.AreEqual(1, service.PutAsyncCallCount);
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(mutationId, service.LastPutRequest.MutationId);
            Assert.AreEqual(1, service.LastPutRequest.PutEntities.Count);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithMutationIdUsesMutationId()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = CreateFluentPut(service);
            var mutationId = MutationId.Create("custom-mutation-id");
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(mutationId, cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(mutationId, service.LastPutRequest.MutationId);
        }

        [TestMethod]
        public async Task ExecuteAsyncPassesAllEdgesCheckForNodes()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("testuser");
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                [user],
                ImmutableList<EdgeByPropCheck>.Empty,
                ImmutableList<string>.Empty,
                false));
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(1, service.LastPutRequest.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(user, service.LastPutRequest.AllEdgesCheckForNodes[0]);
        }

        [TestMethod]
        public async Task ExecuteAsyncPassesEdgeByPropChecks()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var check = new EdgeByPropCheck("EdgeType", "inId", "propName", "propValue", true);
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                ImmutableList<INode>.Empty,
                [check],
                ImmutableList<string>.Empty,
                false));
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(1, service.LastPutRequest.EdgeByPropChecks.Count);
            Assert.AreEqual(check, service.LastPutRequest.EdgeByPropChecks[0]);
        }

        [TestMethod]
        public async Task ExecuteAsyncPassesNoEdgeChecksForNodeIds()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var nodeId = "node-id-1";
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                ImmutableList<INode>.Empty,
                ImmutableList<EdgeByPropCheck>.Empty,
                [nodeId],
                false));
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(1, service.LastPutRequest.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId, service.LastPutRequest.NoEdgeChecksForNodeIds[0]);
        }

        [TestMethod]
        public async Task ExecuteAsyncPassesWithoutNodeEdgeChecks()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                ImmutableList<INode>.Empty,
                ImmutableList<EdgeByPropCheck>.Empty,
                ImmutableList<string>.Empty,
                true));
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.IsTrue(service.LastPutRequest.WithoutNodeEdgeChecks);
        }

        #endregion

        #region WithAllEdgesCheckForNodes Tests

        [TestMethod]
        public void WithAllEdgesCheckForNodesWithParamsAddsNodes()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes(user1, user2);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(user1, result.Query.AllEdgesCheckForNodes[0]);
            Assert.AreEqual(user2, result.Query.AllEdgesCheckForNodes[1]);
        }

        [TestMethod]
        public void WithAllEdgesCheckForNodesWithEnumerableAddsNodes()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            IEnumerable<INode> nodes = new[] { user1, user2 };
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes(nodes);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(user1, result.Query.AllEdgesCheckForNodes[0]);
            Assert.AreEqual(user2, result.Query.AllEdgesCheckForNodes[1]);
        }

        [TestMethod]
        public void WithAllEdgesCheckForNodesPreservesExistingNodes()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                [user1],
                ImmutableList<EdgeByPropCheck>.Empty,
                ImmutableList<string>.Empty,
                false));

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes(user2, user3);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(user1, result.Query.AllEdgesCheckForNodes[0]);
            Assert.AreEqual(user2, result.Query.AllEdgesCheckForNodes[1]);
            Assert.AreEqual(user3, result.Query.AllEdgesCheckForNodes[2]);
        }

        [TestMethod]
        public void WithAllEdgesCheckForNodesPreservesOtherProperties()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("user");
            var entity = CreateTestUser("entity");
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var nodeId = "node-id";
            var fluentPut = new FluentPut(service, new PutQuery(
                [entity],
                ImmutableList<INode>.Empty,
                [check],
                [nodeId],
                true));

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes(user);

            // Assert
            Assert.AreEqual(1, result.Query.PutEntities.Count);
            Assert.AreEqual(entity, result.Query.PutEntities[0]);
            Assert.AreEqual(1, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(check, result.Query.EdgeByPropChecks[0]);
            Assert.AreEqual(1, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId, result.Query.NoEdgeChecksForNodeIds[0]);
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
        }

        #endregion

        #region WithEdgeByPropCheckForNodes Tests

        [TestMethod]
        public void WithEdgeByPropCheckForNodesWithParamsAddsChecks()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var check1 = new EdgeByPropCheck("EdgeType1", "inId1", "prop1", "value1", true);
            var check2 = new EdgeByPropCheck("EdgeType2", "inId2", "prop2", "value2", false);
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithEdgeByPropCheckForNodes(check1, check2);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(check1, result.Query.EdgeByPropChecks[0]);
            Assert.AreEqual(check2, result.Query.EdgeByPropChecks[1]);
        }

        [TestMethod]
        public void WithEdgeByPropCheckForNodesWithEnumerableAddsChecks()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var check1 = new EdgeByPropCheck("EdgeType1", "inId1", "prop1", "value1", true);
            var check2 = new EdgeByPropCheck("EdgeType2", "inId2", "prop2", "value2", false);
            IEnumerable<EdgeByPropCheck> checks = new[] { check1, check2 };
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithEdgeByPropCheckForNodes(checks);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(check1, result.Query.EdgeByPropChecks[0]);
            Assert.AreEqual(check2, result.Query.EdgeByPropChecks[1]);
        }

        [TestMethod]
        public void WithEdgeByPropCheckForNodesPreservesExistingChecks()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var check1 = new EdgeByPropCheck("EdgeType1", "inId1", "prop1", "value1", true);
            var check2 = new EdgeByPropCheck("EdgeType2", "inId2", "prop2", "value2", false);
            var check3 = new EdgeByPropCheck("EdgeType3", "inId3", "prop3", "value3", true);
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                ImmutableList<INode>.Empty,
                [check1],
                ImmutableList<string>.Empty,
                false));

            // Act
            var result = fluentPut.WithEdgeByPropCheckForNodes(check2, check3);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(check1, result.Query.EdgeByPropChecks[0]);
            Assert.AreEqual(check2, result.Query.EdgeByPropChecks[1]);
            Assert.AreEqual(check3, result.Query.EdgeByPropChecks[2]);
        }

        [TestMethod]
        public void WithEdgeByPropCheckForNodesPreservesOtherProperties()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var entity = CreateTestUser("entity");
            var node = CreateTestUser("node");
            var nodeId = "node-id";
            var fluentPut = new FluentPut(service, new PutQuery(
                [entity],
                [node],
                ImmutableList<EdgeByPropCheck>.Empty,
                [nodeId],
                true));

            // Act
            var result = fluentPut.WithEdgeByPropCheckForNodes(check);

            // Assert
            Assert.AreEqual(1, result.Query.PutEntities.Count);
            Assert.AreEqual(entity, result.Query.PutEntities[0]);
            Assert.AreEqual(1, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(node, result.Query.AllEdgesCheckForNodes[0]);
            Assert.AreEqual(1, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId, result.Query.NoEdgeChecksForNodeIds[0]);
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
        }

        #endregion

        #region WithNoEdgesChecksForNodeIds Tests

        [TestMethod]
        public void WithNoEdgesChecksForNodeIdsWithParamsAddsNodeIds()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var nodeId1 = "node-id-1";
            var nodeId2 = "node-id-2";
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithNoEdgesChecksForNodeIds(nodeId1, nodeId2);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId1, result.Query.NoEdgeChecksForNodeIds[0]);
            Assert.AreEqual(nodeId2, result.Query.NoEdgeChecksForNodeIds[1]);
        }

        [TestMethod]
        public void WithNoEdgesChecksForNodeIdsWithEnumerableAddsNodeIds()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var nodeId1 = "node-id-1";
            var nodeId2 = "node-id-2";
            IEnumerable<string> nodeIds = new[] { nodeId1, nodeId2 };
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithNoEdgesChecksForNodeIds(nodeIds);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId1, result.Query.NoEdgeChecksForNodeIds[0]);
            Assert.AreEqual(nodeId2, result.Query.NoEdgeChecksForNodeIds[1]);
        }

        [TestMethod]
        public void WithNoEdgesChecksForNodeIdsPreservesExistingNodeIds()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var nodeId1 = "node-id-1";
            var nodeId2 = "node-id-2";
            var nodeId3 = "node-id-3";
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                ImmutableList<INode>.Empty,
                ImmutableList<EdgeByPropCheck>.Empty,
                [nodeId1],
                false));

            // Act
            var result = fluentPut.WithNoEdgesChecksForNodeIds(nodeId2, nodeId3);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId1, result.Query.NoEdgeChecksForNodeIds[0]);
            Assert.AreEqual(nodeId2, result.Query.NoEdgeChecksForNodeIds[1]);
            Assert.AreEqual(nodeId3, result.Query.NoEdgeChecksForNodeIds[2]);
        }

        [TestMethod]
        public void WithNoEdgesChecksForNodeIdsPreservesOtherProperties()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var nodeId = "node-id";
            var entity = CreateTestUser("entity");
            var node = CreateTestUser("node");
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var fluentPut = new FluentPut(service, new PutQuery(
                [entity],
                [node],
                [check],
                ImmutableList<string>.Empty,
                true));

            // Act
            var result = fluentPut.WithNoEdgesChecksForNodeIds(nodeId);

            // Assert
            Assert.AreEqual(1, result.Query.PutEntities.Count);
            Assert.AreEqual(entity, result.Query.PutEntities[0]);
            Assert.AreEqual(1, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(node, result.Query.AllEdgesCheckForNodes[0]);
            Assert.AreEqual(1, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(check, result.Query.EdgeByPropChecks[0]);
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
        }

        #endregion

        #region WithNoEdgeChecksForAllNodes Tests

        [TestMethod]
        public void WithNoEdgeChecksForAllNodesSetsWithoutNodeEdgeChecksTrue()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithNoEdgeChecksForAllNodes();

            // Assert
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
        }

        [TestMethod]
        public void WithNoEdgeChecksForAllNodesPreservesOtherProperties()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var entity = CreateTestUser("entity");
            var node = CreateTestUser("node");
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var nodeId = "node-id";
            var fluentPut = new FluentPut(service, new PutQuery(
                [entity],
                [node],
                [check],
                [nodeId],
                false));

            // Act
            var result = fluentPut.WithNoEdgeChecksForAllNodes();

            // Assert
            Assert.AreEqual(1, result.Query.PutEntities.Count);
            Assert.AreEqual(entity, result.Query.PutEntities[0]);
            Assert.AreEqual(1, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(node, result.Query.AllEdgesCheckForNodes[0]);
            Assert.AreEqual(1, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(check, result.Query.EdgeByPropChecks[0]);
            Assert.AreEqual(1, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.AreEqual(nodeId, result.Query.NoEdgeChecksForNodeIds[0]);
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
        }

        [TestMethod]
        public void WithNoEdgeChecksForAllNodesOverwritesPreviousValue()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = new FluentPut(service, new PutQuery(
                ImmutableList<IEntity>.Empty,
                ImmutableList<INode>.Empty,
                ImmutableList<EdgeByPropCheck>.Empty,
                ImmutableList<string>.Empty,
                false));

            // Act
            var result = fluentPut.WithNoEdgeChecksForAllNodes();

            // Assert
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
        }

        #endregion

        #region Chaining Tests

        [TestMethod]
        public void ChainingMultipleMethodsPreservesAllValues()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var nodeId = "node-id";
            var entity = CreateTestUser("entity");
            var fluentPut = CreateFluentPutWithEntities(service, entity);

            // Act
            var result = fluentPut
                .WithAllEdgesCheckForNodes(user1)
                .WithEdgeByPropCheckForNodes(check)
                .WithNoEdgesChecksForNodeIds(nodeId)
                .WithAllEdgesCheckForNodes(user2)
                .WithNoEdgeChecksForAllNodes();

            // Assert
            Assert.AreEqual(1, result.Query.PutEntities.Count);
            Assert.AreEqual(2, result.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(1, result.Query.EdgeByPropChecks.Count);
            Assert.AreEqual(1, result.Query.NoEdgeChecksForNodeIds.Count);
            Assert.IsTrue(result.Query.WithoutNodeEdgeChecks);
            Assert.AreSame(service, result.GraphQueryService);
        }

        [TestMethod]
        public void WithMethodsReturnNewFluentPutInstance()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("user");
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes(user);

            // Assert
            Assert.AreNotSame(fluentPut, result);
            Assert.AreSame(service, result.GraphQueryService);
        }

        [TestMethod]
        public void WithMethodsDoNotModifyOriginalFluentPut()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("user");
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes(user);

            // Assert
            Assert.AreEqual(0, fluentPut.Query.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(1, result.Query.AllEdgesCheckForNodes.Count);
        }

        [TestMethod]
        public async Task ExecuteAsyncWithAllPropertiesPassesAllValues()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("user");
            var entity = CreateTestUser("entity");
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var nodeId = "node-id";
            var fluentPut = new FluentPut(service, new PutQuery(
                [entity],
                [user],
                [check],
                [nodeId],
                true));
            var cancellationToken = CancellationToken.None;

            // Act
            await fluentPut.ExecuteAsync(cancellationToken);

            // Assert
            Assert.IsNotNull(service.LastPutRequest);
            Assert.AreEqual(1, service.LastPutRequest.PutEntities.Count);
            Assert.AreEqual(1, service.LastPutRequest.AllEdgesCheckForNodes.Count);
            Assert.AreEqual(1, service.LastPutRequest.EdgeByPropChecks.Count);
            Assert.AreEqual(1, service.LastPutRequest.NoEdgeChecksForNodeIds.Count);
            Assert.IsTrue(service.LastPutRequest.WithoutNodeEdgeChecks);
        }

        #endregion

        #region Service Reference Tests

        [TestMethod]
        public void AllMethodsPreserveGraphQueryService()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var user = CreateTestUser("user");
            var check = new EdgeByPropCheck("EdgeType", "inId", "prop", "value", true);
            var nodeId = "node-id";
            var fluentPut = CreateFluentPut(service);

            // Act & Assert
            Assert.AreSame(service, fluentPut.WithAllEdgesCheckForNodes(user).GraphQueryService);
            Assert.AreSame(service, fluentPut.WithAllEdgesCheckForNodes((IEnumerable<INode>)new[] { user }).GraphQueryService);
            Assert.AreSame(service, fluentPut.WithEdgeByPropCheckForNodes(check).GraphQueryService);
            Assert.AreSame(service, fluentPut.WithEdgeByPropCheckForNodes((IEnumerable<EdgeByPropCheck>)new[] { check }).GraphQueryService);
            Assert.AreSame(service, fluentPut.WithNoEdgesChecksForNodeIds(nodeId).GraphQueryService);
            Assert.AreSame(service, fluentPut.WithNoEdgesChecksForNodeIds((IEnumerable<string>)new[] { nodeId }).GraphQueryService);
            Assert.AreSame(service, fluentPut.WithNoEdgeChecksForAllNodes().GraphQueryService);
        }

        #endregion

        #region Empty Input Tests

        [TestMethod]
        public void WithAllEdgesCheckForNodesWithEmptyArrayAddsNothing()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithAllEdgesCheckForNodes();

            // Assert
            Assert.AreEqual(0, result.Query.AllEdgesCheckForNodes.Count);
        }

        [TestMethod]
        public void WithEdgeByPropCheckForNodesWithEmptyArrayAddsNothing()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithEdgeByPropCheckForNodes();

            // Assert
            Assert.AreEqual(0, result.Query.EdgeByPropChecks.Count);
        }

        [TestMethod]
        public void WithNoEdgesChecksForNodeIdsWithEmptyArrayAddsNothing()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var fluentPut = CreateFluentPut(service);

            // Act
            var result = fluentPut.WithNoEdgesChecksForNodeIds();

            // Assert
            Assert.AreEqual(0, result.Query.NoEdgeChecksForNodeIds.Count);
        }

        #endregion
    }
}
