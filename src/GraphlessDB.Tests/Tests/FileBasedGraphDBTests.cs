/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.IO;
using GraphlessDB.DependencyInjection;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal.Tests;
using GraphlessDB.Query.Services;
using GraphlessDB.Query.Services.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class FileBasedGraphDBTests : GraphDBTests
    {
        private static string? _testStoragePath;

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            _testStoragePath = Path.Combine(Path.GetTempPath(), "GraphlessDB.Tests", Guid.NewGuid().ToString());
            Directory.CreateDirectory(_testStoragePath);
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            if (_testStoragePath != null && Directory.Exists(_testStoragePath))
            {
                try
                {
                    Directory.Delete(_testStoragePath, true);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (_testStoragePath != null && Directory.Exists(_testStoragePath))
            {
                var files = Directory.GetFiles(_testStoragePath, "*", SearchOption.AllDirectories);
                foreach (var file in files)
                {
                    try
                    {
                        File.Delete(file);
                    }
                    catch
                    {
                        // Ignore cleanup errors
                    }
                }
            }
        }

        protected override IServiceCollection ConfigureGraphDBServices(IServiceCollection services)
        {
            if (_testStoragePath == null)
            {
                throw new InvalidOperationException("Test storage path not initialized");
            }

            services
                .AddFileBasedRDFTripleStoreOptions(o =>
                {
                    o.StoragePath = _testStoragePath;
                })
                .AddGraphlessDBWithFileBasedDB()
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, TestGraphGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>();

            return services;
        }
    }
}
