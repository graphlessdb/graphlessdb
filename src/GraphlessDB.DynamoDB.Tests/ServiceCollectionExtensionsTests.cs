/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.DynamoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class ServiceCollectionExtensionsTests
    {
        [TestMethod]
        public void AddGraphlessDBWithDynamoDBRegistersRequiredServices()
        {
            var services = new ServiceCollection();
            services.AddGraphlessDBWithDynamoDB();

            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IAmazonDynamoDBRDFTripleItemService)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IRDFTripleIntegrityChecker)));
            Assert.IsTrue(services.Any(s => s.ServiceType == typeof(IRDFTripleStore<StoreType.Data>)));
        }

        [TestMethod]
        public void AddGraphlessDBWithDynamoDBReturnsServiceCollection()
        {
            var services = new ServiceCollection();
            var result = services.AddGraphlessDBWithDynamoDB();

            Assert.AreSame(services, result);
        }

        [TestMethod]
        public void AddAmazonDynamoDBOptionsConfiguresOptions()
        {
            var services = new ServiceCollection();
            services.AddAmazonDynamoDBOptions(options =>
            {
                options.QuickTransactionsEnabled = false;
                options.TransactGetItemCountMaxValue = 50;
            });
            var provider = services.BuildServiceProvider();

            var options = provider.GetService<IOptions<AmazonDynamoDBOptions>>();
            Assert.IsNotNull(options);
            Assert.AreEqual(false, options.Value.QuickTransactionsEnabled);
            Assert.AreEqual(50, options.Value.TransactGetItemCountMaxValue);
        }

        [TestMethod]
        public void AddAmazonDynamoDBOptionsReturnsServiceCollection()
        {
            var services = new ServiceCollection();
            var result = services.AddAmazonDynamoDBOptions(options =>
            {
                options.QuickTransactionsEnabled = false;
            });

            Assert.AreSame(services, result);
        }

        [TestMethod]
        public void AddAmazonDynamoDBOptionsAllowsMultipleConfigurations()
        {
            var services = new ServiceCollection();
            services.AddAmazonDynamoDBOptions(options =>
            {
                options.TransactGetItemCountMaxValue = 30;
            });
            services.AddAmazonDynamoDBOptions(options =>
            {
                options.TransactGetItemCountMaxValue = 70;
            });
            var provider = services.BuildServiceProvider();

            var options = provider.GetService<IOptions<AmazonDynamoDBOptions>>();
            Assert.IsNotNull(options);
            Assert.AreEqual(70, options.Value.TransactGetItemCountMaxValue);
        }
    }
}
