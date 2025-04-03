/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;
using GraphlessDB.Query.Services.Internal;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.Internal;
using GraphlessDB.Storage.Services.Internal.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB.DependencyInjection
{
    internal static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBWithInMemoryDB(
            this IServiceCollection source)
        {
            return source
                .AddGraphlessDBCore()
                .AddSingleton<IRDFTripleStore<StoreType.Data>, InMemoryRDFTripleStore>()
                .AddSingleton<IInMemoryRDFEventReader, InMemoryRDFEventReader>()
                .AddScoped<IInMemoryNodeEventProcessor, InMemoryNodeEventProcessor>();
        }

        public static IServiceCollection AddGraphlessDBCore(
            this IServiceCollection source)
        {
            return source
                .AddSingleton<IGraphCursorSerializationService, GraphCursorSerializationService>()
                .AddSingleton<IGraphEntitySerializationService, GraphEntitySerializationService>()
                .AddScoped<IGraphSerializationService, GraphSerializationService>()
                .AddScoped<IGraphDB, GraphDB>()
                .AddScoped<IGraphHouseKeepingService, GraphHouseKeepingService>()
                .AddScoped<IRDFTripleFactory, RDFTripleFactory>()
                .AddScoped<IGraphPartitionService, GraphPartitionService>()
                .AddScoped<IGraphEntityTypeService, GraphEntityTypeService>()
                .AddScoped<IMemoryCache, ConcurrentMemoryCache>()
                .AddScoped<IRDFTripleStoreConsumedCapacity, InMemoryRDFTripleStoreConsumedCapacity>()
                .AddScoped<IGraphQueryExecutionService, GraphQueryExecutionService>()
                .AddScoped<IGraphNodeFilterDataLayerService, EmptyGraphNodeFilterDataLayerService>()
                .AddScoped<IGraphNodeFilterService, GraphNodeFilterService>()
                .AddScoped<IGraphEdgeFilterService, GraphEdgeFilterService>()
                .AddScoped<IFromEdgeQueryExecutor, FromEdgeQueryExecutor>()
                .AddScoped<IFromEdgeConnectionQueryExecutor, FromEdgeConnectionQueryExecutor>()
                .AddScoped<IToEdgeConnectionQueryExecutor, ToEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<NodeByIdQuery>, NodeByIdQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<NodeByNodeQuery>, NodeByNodeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<NodeOrDefaultByIdQuery>, NodeOrDefaultByIdQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<NodeVersionByIdQuery>, NodeVersionByIdQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<EdgeByIdQuery>, EdgeByIdQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<EdgeOrDefaultByIdQuery>, EdgeOrDefaultByIdQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<NodeConnectionQuery>, NodeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<InToEdgeConnectionQuery>, InToEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<InToAllEdgeConnectionQuery>, InToAllEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<OutToEdgeConnectionQuery>, OutToEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<OutToAllEdgeConnectionQuery>, OutToAllEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<InFromEdgeConnectionQuery>, InFromEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<InFromEdgeQuery>, InFromEdgeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<OutFromEdgeConnectionQuery>, OutFromEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<OutFromEdgeQuery>, OutFromEdgeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<InAndOutToEdgeConnectionQuery>, InAndOutToEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<InAndOutFromEdgeConnectionQuery>, InAndOutFromEdgeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<SingleNodeQuery>, SingleNodeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<SingleOrDefaultNodeQuery>, SingleOrDefaultNodeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<FirstNodeQuery>, FirstNodeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<FirstOrDefaultNodeQuery>, FirstOrDefaultNodeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<SingleEdgeQuery>, SingleEdgeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<SingleOrDefaultEdgeQuery>, SingleOrDefaultEdgeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<FirstEdgeQuery>, FirstEdgeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<FirstOrDefaultEdgeQuery>, FirstOrDefaultEdgeQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<ZipNodeConnectionQuery>, ZipNodeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<WhereNodeConnectionQuery>, WhereNodeConnectionQueryExecutor>()
                .AddScoped<IGraphQueryNodeExecutionService<WhereEdgeConnectionQuery>, WhereEdgeConnectionQueryExecutor>()
                .AddScoped<IRDFTripleExclusiveStartKeyService, RDFTripleExclusiveStartKeyService>()
                .AddScoped<IGraphQueryService, RDFTripleGraphQueryService>()
                .AddScoped<IRDFTripleStore, RDFTripleStore>()
                .AddScoped<IRDFTripleStore<StoreType.Cached>, CachedRDFTripleStore>();
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

        public static IServiceCollection AddGraphlessDBTypeMapperOptions(this IServiceCollection source, Action<GraphDBAssemblyTypeMapperOptions> configureOptions)
        {
            source
                .AddOptions<GraphDBAssemblyTypeMapperOptions>()
                .Configure(configureOptions)
                .Validate(options =>
                {
                    if (string.IsNullOrWhiteSpace(options.AssemblyName) ||
                        string.IsNullOrWhiteSpace(options.Namespace))
                    {
                        return false;
                    }
                    return true;
                });

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