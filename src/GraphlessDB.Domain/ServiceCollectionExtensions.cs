/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Domain.Graph.Services;
using GraphlessDB.Storage.Services;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBDomainGraph(
            this IServiceCollection source)
        {
            return source
                .AddSingleton<IGraphCursorSerializationService, GraphCursorSerializationService>()
                .AddSingleton<IGraphEntitySerializationService, GraphEntitySerializationService>()
                .AddScoped<IGraphSerializationService, GraphSerializationService>()
                .AddScoped<IRDFTripleFactory, RDFTripleFactory>()
                .AddScoped<IGraphPartitionService, GraphPartitionService>()
                .AddScoped<IGraphEntityTypeService, GraphEntityTypeNativeService>()
                .AddScoped<IMemoryCache, ConcurrentMemoryCache>()
                .AddScoped<IRDFTripleExclusiveStartKeyService, RDFTripleExclusiveStartKeyService>()
                .AddScoped<IGraphQueryService, RDFTripleGraphQueryService>();
        }

    }
}
