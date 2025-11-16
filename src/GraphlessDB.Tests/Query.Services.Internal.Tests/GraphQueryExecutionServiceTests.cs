/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal.Tests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class GraphQueryExecutionServiceTests
    {
        [TestMethod]
        public async Task MutateAsyncWithoutReturnValueCanExecuteSuccessfully()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var executed = false;
            await executionService.MutateAsync(async () =>
            {
                await Task.Delay(10, cancellationToken);
                executed = true;
            }, cancellationToken);

            Assert.IsTrue(executed);
        }

        [TestMethod]
        public async Task MutateAsyncWithReturnValueCanExecuteSuccessfully()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var result = await executionService.MutateAsync(async () =>
            {
                await Task.Delay(10, cancellationToken);
                return 42;
            }, cancellationToken);

            Assert.AreEqual(42, result);
        }

        [TestMethod]
        public async Task MutateAsyncRetriesOnThroughputExceededException()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var attemptCount = 0;
            var result = await executionService.MutateAsync(async () =>
            {
                await Task.CompletedTask;
                attemptCount++;
                if (attemptCount < 3)
                {
                    throw new GraphlessDBThroughputExceededException("Throughput exceeded");
                }
                return true;
            }, cancellationToken);

            Assert.IsTrue(result);
            Assert.AreEqual(3, attemptCount);
        }

        [TestMethod]
        public async Task MutateAsyncRetriesOnConcurrencyException()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var attemptCount = 0;
            var result = await executionService.MutateAsync(async () =>
            {
                await Task.CompletedTask;
                attemptCount++;
                if (attemptCount < 3)
                {
                    throw new GraphlessDBConcurrencyException("Concurrency issue");
                }
                return true;
            }, cancellationToken);

            Assert.IsTrue(result);
            Assert.AreEqual(3, attemptCount);
        }

        [TestMethod]
        public async Task MutateAsyncRetriesOnHttpRequestException()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var attemptCount = 0;
            var result = await executionService.MutateAsync(async () =>
            {
                await Task.CompletedTask;
                attemptCount++;
                if (attemptCount < 3)
                {
                    throw new HttpRequestException("Network error");
                }
                return true;
            }, cancellationToken);

            Assert.IsTrue(result);
            Assert.AreEqual(3, attemptCount);
        }

        [TestMethod]
        public async Task MutateAsyncThrowsLastExceptionOnCancellation()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            var cancellationToken = cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var exceptionThrown = false;
            try
            {
                await executionService.MutateAsync(async () =>
                {
                    await Task.Delay(50);
                    throw new GraphlessDBConcurrencyException("Concurrency issue");
                }, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                exceptionThrown = true;
            }
            catch (GraphlessDBConcurrencyException)
            {
                exceptionThrown = true;
            }

            Assert.IsTrue(exceptionThrown);
        }

        [TestMethod]
        public async Task MutateAsyncPropagatesNonRetriableExceptions()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await executionService.MutateAsync(async () =>
                {
                    await Task.CompletedTask;
                    throw new InvalidOperationException("Non-retriable exception");
                }, cancellationToken);
            });
        }

        [TestMethod]
        public async Task MutateAsyncWithoutReturnValueCallsUnderlyingOperation()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var callCount = 0;
            await executionService.MutateAsync(async () =>
            {
                await Task.CompletedTask;
                callCount++;
            }, cancellationToken);

            Assert.AreEqual(1, callCount);
        }

        [TestMethod]
        public async Task MutateAsyncRetriesMultipleTimesWithIncreasingBackoff()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var attemptCount = 0;
            var result = await executionService.MutateAsync(async () =>
            {
                await Task.CompletedTask;
                attemptCount++;
                if (attemptCount < 5)
                {
                    throw new GraphlessDBThroughputExceededException("Throughput exceeded");
                }
                return true;
            }, cancellationToken);

            Assert.IsTrue(result);
            Assert.AreEqual(5, attemptCount);
        }

        [TestMethod]
        public async Task MutateAsyncMixedRetriesWithDifferentExceptionTypes()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var cancellationToken = Debugger.IsAttached ? CancellationToken.None : cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var attemptCount = 0;
            var result = await executionService.MutateAsync(async () =>
            {
                await Task.CompletedTask;
                attemptCount++;
                if (attemptCount == 1)
                {
                    throw new GraphlessDBThroughputExceededException("Throughput exceeded");
                }
                else if (attemptCount == 2)
                {
                    throw new GraphlessDBConcurrencyException("Concurrency issue");
                }
                else if (attemptCount == 3)
                {
                    throw new HttpRequestException("Network error");
                }
                return true;
            }, cancellationToken);

            Assert.IsTrue(result);
            Assert.AreEqual(4, attemptCount);
        }

        [TestMethod]
        public async Task MutateAsyncThrowsLastExceptionWhenCancellationRequestedDuringRetry()
        {
            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
            var cancellationToken = cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            var attemptCount = 0;
            var exceptionThrown = false;
            try
            {
                await executionService.MutateAsync(async () =>
                {
                    attemptCount++;
                    await Task.CompletedTask;
                    throw new GraphlessDBConcurrencyException("Concurrency issue");
                }, cancellationToken);
            }
            catch (GraphlessDBConcurrencyException)
            {
                exceptionThrown = true;
            }
            catch (TaskCanceledException)
            {
                exceptionThrown = true;
            }

            Assert.IsTrue(exceptionThrown);
            Assert.IsTrue(attemptCount >= 1);
        }

        [TestMethod]
        public async Task MutateAsyncThrowsOperationExceptionWhenCancelledWithoutException()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var services = new ServiceCollection();

            services
                .AddGraphlessDBGraphOptions(o =>
                {
                    o.TableName = "TestTable";
                    o.GraphName = "a";
                    o.PartitionCount = 1;
                });

            var serviceProvider = services
                .AddSingleton<IGraphSettingsService, GraphDBSettingsService>()
                .AddSingleton<IGraphQueryablePropertyService, EmptyGraphQueryablePropertyService>()
                .AddSingleton<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddSingleton<IGraphEventService, EmptyGraphDBEventService>()
                .AddLogging()
                .AddGraphlessDBWithInMemoryDB()
                .BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });

            var executionService = serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetRequiredService<IGraphQueryExecutionService>();

            cancellationTokenSource.Cancel();

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await executionService.MutateAsync(async () =>
                {
                    await Task.CompletedTask;
                    return true;
                }, cancellationToken);
            });
        }
    }
}
