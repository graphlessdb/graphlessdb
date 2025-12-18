/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.FileBased;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBFileBasedStorage(
            this IServiceCollection source)
        {
            return source
                .AddScoped<IRDFTripleStore, RDFTripleStore>()
                .AddScoped<IRDFTripleStore<StoreType.Cached>, CachedRDFTripleStore>()
                .AddSingleton<IRDFTripleStore<StoreType.Data>, FileBasedRDFTripleStore>()
                .AddSingleton<IFileBasedRDFEventReader, FileBasedRDFEventReader>()
                .AddScoped<IFileBasedNodeEventProcessor, FileBasedNodeEventProcessor>();
        }

        public static IServiceCollection AddFileBasedRDFTripleStoreOptions(this IServiceCollection source, Action<FileBasedRDFTripleStoreOptions> configureOptions)
        {
            source
                .AddOptions<FileBasedRDFTripleStoreOptions>()
                .Configure(configureOptions)
                .Validate(options =>
                {
                    return !string.IsNullOrWhiteSpace(options.StoragePath);
                });

            return source;
        }
    }
}
