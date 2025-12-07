/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.IO;
using System.Linq;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.Internal.FileBased;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DependencyInjection.Tests
{
    [TestClass]
    public sealed class ServiceCollectionExtensionsTests
    {
        [TestMethod]
        public void AddGraphlessDBWithFileBasedDBRegistersFileBasedServices()
        {
            var services = new ServiceCollection();
            var tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

            try
            {
                Directory.CreateDirectory(tempPath);

                services
                    .AddGraphlessDBGraphOptions(o =>
                    {
                        o.TableName = "TestTable";
                        o.GraphName = "test";
                        o.PartitionCount = 1;
                    })
                    .AddFileBasedRDFTripleStoreOptions(o =>
                    {
                        o.StoragePath = tempPath;
                    })
                    .AddGraphlessDBWithFileBasedDB();

                var serviceDescriptors = services.ToArray();

                Assert.IsTrue(serviceDescriptors.Any(sd => sd.ServiceType == typeof(IRDFTripleStore<StoreType.Data>) && sd.ImplementationType == typeof(FileBasedRDFTripleStore)),
                    "FileBasedRDFTripleStore should be registered as IRDFTripleStore<StoreType.Data>");

                Assert.IsTrue(serviceDescriptors.Any(sd => sd.ServiceType == typeof(IFileBasedRDFEventReader)),
                    "IFileBasedRDFEventReader should be registered");

                Assert.IsTrue(serviceDescriptors.Any(sd => sd.ServiceType == typeof(IFileBasedNodeEventProcessor)),
                    "IFileBasedNodeEventProcessor should be registered");
            }
            finally
            {
                if (Directory.Exists(tempPath))
                {
                    Directory.Delete(tempPath, true);
                }
            }
        }

        [TestMethod]
        public void AddFileBasedRDFTripleStoreOptionsValidatesStoragePath()
        {
            var services = new ServiceCollection();

            services
                .AddFileBasedRDFTripleStoreOptions(o =>
                {
                    o.StoragePath = "";
                });

            var serviceProvider = services.BuildServiceProvider();

            Assert.ThrowsException<OptionsValidationException>(() =>
            {
                var options = serviceProvider.GetRequiredService<IOptions<FileBasedRDFTripleStoreOptions>>().Value;
            });
        }
    }
}
