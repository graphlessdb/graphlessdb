/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
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
    public sealed class GraphDBExtensionsTests
    {
        private sealed class MockGraphQueryExecutionService : IGraphQueryExecutionService
        {
            public int MutateAsyncCallCount { get; private set; }
            public int MutateAsyncGenericCallCount { get; private set; }
            public Func<Task>? LastMutateOperation { get; private set; }
            public object? LastMutateGenericOperation { get; private set; }

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
                MutateAsyncCallCount++;
                LastMutateOperation = operation;
                return operation();
            }

            public Task<T> MutateAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
            {
                MutateAsyncGenericCallCount++;
                LastMutateGenericOperation = operation;
                return operation();
            }

            public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockGraphDB : IGraphDB
        {
            public MockGraphDB(IGraphQueryExecutionService queryExecutionService)
            {
                QueryExecutionService = queryExecutionService;
            }

            public IGraphQueryExecutionService QueryExecutionService { get; }

            public IGraphNodeFilterService NodeFilterService => throw new NotImplementedException();

            public IGraphHouseKeepingService HouseKeepingService => throw new NotImplementedException();
        }

        [TestMethod]
        public async Task MutateAsyncCallsQueryExecutionServiceMutateAsync()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var graphDB = new MockGraphDB(service);
            var operationCalled = false;
            Func<Task> operation = () =>
            {
                operationCalled = true;
                return Task.CompletedTask;
            };
            var cancellationToken = CancellationToken.None;

            // Act
            await graphDB.MutateAsync(operation, cancellationToken);

            // Assert
            Assert.AreEqual(1, service.MutateAsyncCallCount);
            Assert.IsTrue(operationCalled);
            Assert.AreEqual(operation, service.LastMutateOperation);
        }

        [TestMethod]
        public async Task MutateAsyncGenericCallsQueryExecutionServiceMutateAsync()
        {
            // Arrange
            var service = new MockGraphQueryExecutionService();
            var graphDB = new MockGraphDB(service);
            var operationCalled = false;
            var expectedResult = 42;
            Func<Task<int>> operation = () =>
            {
                operationCalled = true;
                return Task.FromResult(expectedResult);
            };
            var cancellationToken = CancellationToken.None;

            // Act
            var result = await graphDB.MutateAsync(operation, cancellationToken);

            // Assert
            Assert.AreEqual(1, service.MutateAsyncGenericCallCount);
            Assert.IsTrue(operationCalled);
            Assert.AreEqual(expectedResult, result);
            Assert.AreEqual(operation, service.LastMutateGenericOperation);
        }
    }
}
