/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBWithInMemoryDB(
            this IServiceCollection source)
        {
            return source
                .AddScoped<IGraphDB, GraphDB>()
                .AddGraphlessDBDomainGraph()
                .AddGraphlessDBQuery()
                .AddGraphlessDBInMemoryStorage();
        }

        public static IServiceCollection AddGraphlessDBWithFileBasedDB(
            this IServiceCollection source)
        {
            return source
                .AddScoped<IGraphDB, GraphDB>()
                .AddGraphlessDBDomainGraph()
                .AddGraphlessDBQuery()
                .AddGraphlessDBFileBasedStorage();
        }

        public static IServiceCollection AddGraphlessDBGraphOptions(this IServiceCollection source, Action<GraphOptions> configureOptions)
        {
            source
                .AddOptions<GraphOptions>()
                .Configure(configureOptions)
                .Validate(options =>
                {
                    return !(string.IsNullOrWhiteSpace(options.TableName) ||
                        string.IsNullOrWhiteSpace(options.GraphName) ||
                        options.PartitionCount < 1);
                });

            return source;
        }

        public static IServiceCollection AddGraphEntityTypeNativeServiceOptions(this IServiceCollection source, Action<GraphEntityTypeNativeServiceOptions> configureOptions)
        {
            source
                .AddOptions<GraphEntityTypeNativeServiceOptions>()
                .Configure(configureOptions);

            return source;
        }

        public static IServiceCollection AddGraphlessDBEntitySerializerOptions(this IServiceCollection source, Action<GraphEntitySerializationServiceOptions> configureOptions)
        {
            source
                .AddOptions<GraphEntitySerializationServiceOptions>()
                .Configure(configureOptions);

            return source;
        }
    }
}
