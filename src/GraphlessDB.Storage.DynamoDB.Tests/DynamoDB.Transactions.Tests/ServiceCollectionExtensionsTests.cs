/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Linq;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class ServiceCollectionExtensionsTests
    {
        [TestMethod]
        public void AddDynamoDBTransactionsCoreRegistersRequiredServices()
        {
            var services = new ServiceCollection();
            services.AddDynamoDBTransactionsCore();

            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IAmazonDynamoDBWithTransactions)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(ITransactionServiceEvents)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IIsolatedGetItemService<CommittedIsolationLevelServiceType>)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IIsolatedGetItemService<UnCommittedIsolationLevelServiceType>)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IRequestService)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IRequestRecordSerializer)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IVersionedItemStore)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IAmazonDynamoDBKeyService)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IFullyAppliedRequestService)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(ITableSchemaService)));
        }

        [TestMethod]
        public void AddDynamoDBTransactionsCoreReturnsServiceCollection()
        {
            var services = new ServiceCollection();
            var result = services.AddDynamoDBTransactionsCore();

            Assert.AreSame(services, result);
        }

        [TestMethod]
        public void AddDynamoDBTransactionsCoreRegistersServicesAsScoped()
        {
            var services = new ServiceCollection();
            services.AddDynamoDBTransactionsCore();

            Assert.IsTrue(services.All(s => s.Lifetime == ServiceLifetime.Scoped));
        }

        [TestMethod]
        public void AddDynamoDBTransactionsWithDefaultStorageRegistersRequiredServices()
        {
            var services = new ServiceCollection();
            services.AddDynamoDBTransactionsWithDefaultStorage();

            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(ITransactionStore)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IItemImageStore)));
        }

        [TestMethod]
        public void AddDynamoDBTransactionsWithDefaultStorageRegistersTransactionStoreImplementation()
        {
            var services = new ServiceCollection();
            services.AddDynamoDBTransactionsWithDefaultStorage();

            var transactionStoreDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(ITransactionStore));
            Assert.IsNotNull(transactionStoreDescriptor);
            Assert.AreEqual(typeof(DefaultTransactionStore), transactionStoreDescriptor.ImplementationType);
        }

        [TestMethod]
        public void AddDynamoDBTransactionsWithDefaultStorageRegistersItemImageStoreImplementation()
        {
            var services = new ServiceCollection();
            services.AddDynamoDBTransactionsWithDefaultStorage();

            var itemImageStoreDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(IItemImageStore));
            Assert.IsNotNull(itemImageStoreDescriptor);
            Assert.AreEqual(typeof(DefaultItemImageStore), itemImageStoreDescriptor.ImplementationType);
        }

        [TestMethod]
        public void AddDynamoDBTransactionsWithDefaultStorageReturnsServiceCollection()
        {
            var services = new ServiceCollection();
            var result = services.AddDynamoDBTransactionsWithDefaultStorage();

            Assert.AreSame(services, result);
        }

        [TestMethod]
        public void AddDynamoDBTransactionsWithDefaultStorageIncludesCoreServices()
        {
            var services = new ServiceCollection();
            services.AddDynamoDBTransactionsWithDefaultStorage();

            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IAmazonDynamoDBWithTransactions)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(ITransactionServiceEvents)));
        }
    }
}
