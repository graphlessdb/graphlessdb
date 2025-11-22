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
using Amazon.DynamoDBv2.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBKeyServiceTests
    {
        private sealed class MockTableSchemaService : ITableSchemaService
        {
            public Func<string, CancellationToken, Task<ImmutableList<KeySchemaElement>>> GetTableSchemaAsyncFunc { get; set; } =
                (tableName, ct) => Task.FromResult(ImmutableList<KeySchemaElement>.Empty);

            public Task<ImmutableList<KeySchemaElement>> GetTableSchemaAsync(string tableName, CancellationToken cancellationToken)
            {
                return GetTableSchemaAsyncFunc(tableName, cancellationToken);
            }
        }

        private static AmazonDynamoDBKeyService CreateService(MockTableSchemaService? tableSchemaService = null)
        {
            return new AmazonDynamoDBKeyService(tableSchemaService ?? new MockTableSchemaService());
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncReturnsKeyMapWithSinglePartitionKey()
        {
            var schema = ImmutableList<KeySchemaElement>.Empty
                .Add(new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" });

            var mockSchemaService = new MockTableSchemaService
            {
                GetTableSchemaAsyncFunc = (tableName, ct) => Task.FromResult(schema)
            };

            var service = CreateService(mockSchemaService);
            var item = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", new AttributeValue { S = "test-id" })
                .Add("Data", new AttributeValue { S = "test-data" });

            var result = await service.CreateKeyMapAsync("TestTable", item, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.IsTrue(result.ContainsKey("Id"));
            Assert.AreEqual("test-id", result["Id"].S);
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncReturnsKeyMapWithPartitionAndSortKey()
        {
            var schema = ImmutableList<KeySchemaElement>.Empty
                .Add(new KeySchemaElement { AttributeName = "PK", KeyType = "HASH" })
                .Add(new KeySchemaElement { AttributeName = "SK", KeyType = "RANGE" });

            var mockSchemaService = new MockTableSchemaService
            {
                GetTableSchemaAsyncFunc = (tableName, ct) => Task.FromResult(schema)
            };

            var service = CreateService(mockSchemaService);
            var item = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("PK", new AttributeValue { S = "partition-key" })
                .Add("SK", new AttributeValue { S = "sort-key" })
                .Add("Data", new AttributeValue { S = "test-data" });

            var result = await service.CreateKeyMapAsync("TestTable", item, CancellationToken.None);

            Assert.AreEqual(2, result.Count);
            Assert.IsTrue(result.ContainsKey("PK"));
            Assert.IsTrue(result.ContainsKey("SK"));
            Assert.AreEqual("partition-key", result["PK"].S);
            Assert.AreEqual("sort-key", result["SK"].S);
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncDoesNotIncludeNonKeyAttributes()
        {
            var schema = ImmutableList<KeySchemaElement>.Empty
                .Add(new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" });

            var mockSchemaService = new MockTableSchemaService
            {
                GetTableSchemaAsyncFunc = (tableName, ct) => Task.FromResult(schema)
            };

            var service = CreateService(mockSchemaService);
            var item = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", new AttributeValue { S = "test-id" })
                .Add("Data", new AttributeValue { S = "test-data" })
                .Add("Timestamp", new AttributeValue { N = "123456" });

            var result = await service.CreateKeyMapAsync("TestTable", item, CancellationToken.None);

            Assert.AreEqual(1, result.Count);
            Assert.IsTrue(result.ContainsKey("Id"));
            Assert.IsFalse(result.ContainsKey("Data"));
            Assert.IsFalse(result.ContainsKey("Timestamp"));
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncPassesTableNameToSchemaService()
        {
            string? capturedTableName = null;
            var schema = ImmutableList<KeySchemaElement>.Empty
                .Add(new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" });

            var mockSchemaService = new MockTableSchemaService
            {
                GetTableSchemaAsyncFunc = (tableName, ct) =>
                {
                    capturedTableName = tableName;
                    return Task.FromResult(schema);
                }
            };

            var service = CreateService(mockSchemaService);
            var item = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", new AttributeValue { S = "test-id" });

            await service.CreateKeyMapAsync("MyTestTable", item, CancellationToken.None);

            Assert.AreEqual("MyTestTable", capturedTableName);
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncPassesCancellationTokenToSchemaService()
        {
            CancellationToken capturedToken = default;
            var schema = ImmutableList<KeySchemaElement>.Empty
                .Add(new KeySchemaElement { AttributeName = "Id", KeyType = "HASH" });

            var mockSchemaService = new MockTableSchemaService
            {
                GetTableSchemaAsyncFunc = (tableName, ct) =>
                {
                    capturedToken = ct;
                    return Task.FromResult(schema);
                }
            };

            var service = CreateService(mockSchemaService);
            var item = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Id", new AttributeValue { S = "test-id" });

            using var cts = new CancellationTokenSource();
            await service.CreateKeyMapAsync("TestTable", item, cts.Token);

            Assert.AreEqual(cts.Token, capturedToken);
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncReturnsEmptyMapForEmptySchema()
        {
            var schema = ImmutableList<KeySchemaElement>.Empty;

            var mockSchemaService = new MockTableSchemaService
            {
                GetTableSchemaAsyncFunc = (tableName, ct) => Task.FromResult(schema)
            };

            var service = CreateService(mockSchemaService);
            var item = ImmutableDictionary<string, AttributeValue>.Empty
                .Add("Data", new AttributeValue { S = "test-data" });

            var result = await service.CreateKeyMapAsync("TestTable", item, CancellationToken.None);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncThrowsArgumentNullExceptionWhenTableNameIsNull()
        {
            var service = CreateService();
            var item = ImmutableDictionary<string, AttributeValue>.Empty;

            await Assert.ThrowsExceptionAsync<ArgumentNullException>(async () =>
            {
                await service.CreateKeyMapAsync(null!, item, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task CreateKeyMapAsyncThrowsArgumentNullExceptionWhenItemIsNull()
        {
            var service = CreateService();

            await Assert.ThrowsExceptionAsync<ArgumentNullException>(async () =>
            {
                await service.CreateKeyMapAsync("TestTable", null!, CancellationToken.None);
            });
        }
    }
}
