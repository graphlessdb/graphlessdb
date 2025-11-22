/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Internal.Tests
{
    [TestClass]
    public sealed class InMemoryFullyAppliedRequestServiceTests
    {
        [TestMethod]
        public async Task IsFullyAppliedAsyncReturnsFalseForNonExistentKey()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key = new TransactionVersion("tx1", 1);

            var result = await service.IsFullyAppliedAsync(key, CancellationToken.None);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFullyAppliedAsyncReturnsTrueForExistingKey()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key = new TransactionVersion("tx1", 1);

            await service.SetFullyAppliedAsync(key, CancellationToken.None);
            var result = await service.IsFullyAppliedAsync(key, CancellationToken.None);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task SetFullyAppliedAsyncAddsNewKey()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key = new TransactionVersion("tx1", 1);

            await service.SetFullyAppliedAsync(key, CancellationToken.None);
            var result = await service.IsFullyAppliedAsync(key, CancellationToken.None);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task SetFullyAppliedAsyncIsIdempotent()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key = new TransactionVersion("tx1", 1);

            await service.SetFullyAppliedAsync(key, CancellationToken.None);
            await service.SetFullyAppliedAsync(key, CancellationToken.None);
            var result = await service.IsFullyAppliedAsync(key, CancellationToken.None);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task CanHandleMultipleDifferentKeys()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key1 = new TransactionVersion("tx1", 1);
            var key2 = new TransactionVersion("tx2", 1);
            var key3 = new TransactionVersion("tx1", 2);

            await service.SetFullyAppliedAsync(key1, CancellationToken.None);
            await service.SetFullyAppliedAsync(key2, CancellationToken.None);

            Assert.IsTrue(await service.IsFullyAppliedAsync(key1, CancellationToken.None));
            Assert.IsTrue(await service.IsFullyAppliedAsync(key2, CancellationToken.None));
            Assert.IsFalse(await service.IsFullyAppliedAsync(key3, CancellationToken.None));
        }

        [TestMethod]
        public async Task IsFullyAppliedAsyncWithCancellationToken()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key = new TransactionVersion("tx1", 1);
            using var cts = new CancellationTokenSource();

            var result = await service.IsFullyAppliedAsync(key, cts.Token);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task SetFullyAppliedAsyncWithCancellationToken()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key = new TransactionVersion("tx1", 1);
            using var cts = new CancellationTokenSource();

            await service.SetFullyAppliedAsync(key, cts.Token);
            var result = await service.IsFullyAppliedAsync(key, CancellationToken.None);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task TransactionVersionEqualityMatters()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key1 = new TransactionVersion("tx1", 1);
            var key2 = new TransactionVersion("tx1", 1);

            await service.SetFullyAppliedAsync(key1, CancellationToken.None);
            var result = await service.IsFullyAppliedAsync(key2, CancellationToken.None);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task DifferentVersionsAreTreatedSeparately()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key1 = new TransactionVersion("tx1", 1);
            var key2 = new TransactionVersion("tx1", 2);

            await service.SetFullyAppliedAsync(key1, CancellationToken.None);

            Assert.IsTrue(await service.IsFullyAppliedAsync(key1, CancellationToken.None));
            Assert.IsFalse(await service.IsFullyAppliedAsync(key2, CancellationToken.None));
        }

        [TestMethod]
        public async Task DifferentTransactionIdsAreTreatedSeparately()
        {
            var service = new InMemoryFullyAppliedRequestService();
            var key1 = new TransactionVersion("tx1", 1);
            var key2 = new TransactionVersion("tx2", 1);

            await service.SetFullyAppliedAsync(key1, CancellationToken.None);

            Assert.IsTrue(await service.IsFullyAppliedAsync(key1, CancellationToken.None));
            Assert.IsFalse(await service.IsFullyAppliedAsync(key2, CancellationToken.None));
        }
    }
}
