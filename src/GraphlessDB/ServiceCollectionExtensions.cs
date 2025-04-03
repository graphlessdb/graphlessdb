/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBWithInMemoryDB(this IServiceCollection source)
        {
            return DependencyInjection.ServiceCollectionExtensions.AddGraphlessDBWithInMemoryDB(source);
        }

        public static IServiceCollection AddGraphlessDBCore(this IServiceCollection source)
        {
            return DependencyInjection.ServiceCollectionExtensions.AddGraphlessDBCore(source);
        }

        public static IServiceCollection AddGraphlessDBGraphOptions(this IServiceCollection source, Action<GraphOptions> configureOptions)
        {
            return DependencyInjection.ServiceCollectionExtensions.AddGraphlessDBGraphOptions(source, configureOptions);
        }

        public static IServiceCollection AddGraphlessDBTypeMapperOptions(this IServiceCollection source, Action<GraphDBAssemblyTypeMapperOptions> configureOptions)
        {
            return DependencyInjection.ServiceCollectionExtensions.AddGraphlessDBTypeMapperOptions(source, configureOptions);
        }

        public static IServiceCollection AddGraphlessDBEntitySerializerOptions(this IServiceCollection source, Action<GraphEntitySerializationServiceOptions> configureOptions)
        {
            return DependencyInjection.ServiceCollectionExtensions.AddGraphlessDBEntitySerializerOptions(source, configureOptions);
        }
    }
}