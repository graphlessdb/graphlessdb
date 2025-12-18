/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBInMemoryStorage(
            this IServiceCollection source)
        {
            return source
                .AddScoped<IRDFTripleStore, RDFTripleStore>()
                .AddScoped<IRDFTripleStore<StoreType.Cached>, CachedRDFTripleStore>()
                .AddSingleton<IRDFTripleStore<StoreType.Data>, InMemoryRDFTripleStore>()
                .AddSingleton<IInMemoryRDFEventReader, InMemoryRDFEventReader>()
                .AddScoped<IInMemoryNodeEventProcessor, InMemoryNodeEventProcessor>();
        }
    }
}
