/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Query;
using GraphlessDB.Query.Datalog.Core;
using GraphlessDB.Query.Datalog.Execution;
using GraphlessDB.Query.Services;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBQuery(
            this IServiceCollection source)
        {
            return source
                // .AddScoped<IRDFTripleStoreConsumedCapacity, InMemoryRDFTripleStoreConsumedCapacity>()
                .AddScoped<IGraphHouseKeepingService, GraphHouseKeepingService>()
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
                // Datalog query services
                .AddScoped<IGraphQueryNodeExecutionService<DatalogQuery>, DatalogQueryExecutor>()
                .AddScoped<DatalogPatternCompiler>()
                .AddScoped<DatalogRecursiveExecutor>()
                ;
        }


        // public static IServiceCollection AddGraphEntityTypeNativeServiceOptions(this IServiceCollection source, Action<GraphEntityTypeNativeServiceOptions> configureOptions)
        // {
        //     source
        //         .AddOptions<GraphEntityTypeNativeServiceOptions>()
        //         .Configure(configureOptions);

        //     return source;
        // }

        // public static IServiceCollection AddGraphlessDBEntitySerializerOptions(this IServiceCollection source, Action<GraphEntitySerializationServiceOptions> configureOptions)
        // {
        //     source
        //         .AddOptions<GraphEntitySerializationServiceOptions>()
        //         .Configure(configureOptions);

        //     return source;
        // }

        // public static IServiceCollection AddFileBasedRDFTripleStoreOptions(this IServiceCollection source, Action<FileBasedRDFTripleStoreOptions> configureOptions)
        // {
        //     source
        //         .AddOptions<FileBasedRDFTripleStoreOptions>()
        //         .Configure(configureOptions)
        //         .Validate(options =>
        //         {
        //             return !string.IsNullOrWhiteSpace(options.StoragePath);
        //         });

        //     return source;
        // }
    }
}
