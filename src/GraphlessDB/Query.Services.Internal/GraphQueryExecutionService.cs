/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Logging;
using GraphlessDB.Threading;
using Microsoft.Extensions.Logging;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class GraphQueryExecutionService(
        IGraphQueryService graphDataQueryService,
        IGraphQueryNodeExecutionService<NodeByIdQuery> nodeByIdQueryExecutor,
        IGraphQueryNodeExecutionService<NodeOrDefaultByIdQuery> nodeOrDefaultByIdQueryExecutor,
        IGraphQueryNodeExecutionService<NodeVersionByIdQuery> nodeVersionByIdQueryExecutor,
        IGraphQueryNodeExecutionService<EdgeByIdQuery> edgeByIdQueryExecutor,
        IGraphQueryNodeExecutionService<EdgeOrDefaultByIdQuery> edgeOrDefaultByIdQueryExecutor,
        IGraphQueryNodeExecutionService<NodeConnectionQuery> nodeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<InToEdgeConnectionQuery> inToEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<InToAllEdgeConnectionQuery> inToAllEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<OutToEdgeConnectionQuery> outToEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<OutToAllEdgeConnectionQuery> outToAllEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<InFromEdgeConnectionQuery> inFromEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<InFromEdgeQuery> inFromEdgeQueryExecutor,
        IGraphQueryNodeExecutionService<OutFromEdgeConnectionQuery> outFromEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<OutFromEdgeQuery> outFromEdgeQueryExecutor,
        IGraphQueryNodeExecutionService<InAndOutToEdgeConnectionQuery> inAndOutToEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<InAndOutFromEdgeConnectionQuery> inAndOutFromEdgeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<SingleNodeQuery> singleNodeQueryExecutor,
        IGraphQueryNodeExecutionService<SingleOrDefaultNodeQuery> singleOrDefaultNodeQueryExecutor,
        IGraphQueryNodeExecutionService<FirstNodeQuery> firstNodeQueryExecutor,
        IGraphQueryNodeExecutionService<FirstOrDefaultNodeQuery> firstOrDefaultNodeQueryExecutor,
        IGraphQueryNodeExecutionService<SingleEdgeQuery> singleEdgeQueryExecutor,
        IGraphQueryNodeExecutionService<SingleOrDefaultEdgeQuery> singleOrDefaultEdgeQueryExecutor,
        IGraphQueryNodeExecutionService<FirstEdgeQuery> firstEdgeQueryExecutor,
        IGraphQueryNodeExecutionService<FirstOrDefaultEdgeQuery> firstOrDefaultEdgeQueryExecutor,
        IGraphQueryNodeExecutionService<NodeByNodeQuery> nodeByNodeQueryExecutor,
        IGraphQueryNodeExecutionService<ZipNodeConnectionQuery> zipNodeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<WhereNodeConnectionQuery> whereNodeConnectionQueryExecutor,
        IGraphQueryNodeExecutionService<WhereEdgeConnectionQuery> whereEdgeConnectionQueryExecutor,
        IGraphCursorSerializationService cursorSerializer,
        ILogger<GraphQueryExecutionService> logger) : IGraphQueryExecutionService
    {
        private readonly Random _rnd = new();

        public async Task<GraphExecutionContext> GetAsync(
            ImmutableTree<string, GraphQueryNode> query,
            CancellationToken cancellationToken)
        {
            return await Retry.RunAsync(async () =>
            {
                try
                {
                    var context = new GraphExecutionContext(
                        this,
                        query,
                        ImmutableDictionary<string, GraphResult>.Empty);

                    context = ApplyCursor(context);

                    context = EnsureRunnable(context);

                    while (TryGetNextKey(context, out var key))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        context = await ExecuteQueryItemAsync(context, key, cancellationToken);

                        context = EnsureRunnable(context);
                    }

                    return context;
                }
                catch (Exception ex)
                {
                    logger.GraphQueryServiceError(ex);
                    throw;
                }
            }, GraphQueryServiceRetry.RetryOptions, cancellationToken);
        }

        public async Task PutAsync(PutRequest request, CancellationToken cancellationToken)
        {
            await graphDataQueryService.PutAsync(request, cancellationToken);
        }

        public async Task ClearAsync(CancellationToken cancellationToken)
        {
            await graphDataQueryService.ClearAsync(cancellationToken);
        }

        private GraphExecutionContext ApplyCursor(GraphExecutionContext context)
        {
            if (!TryGetRootCursorKey(context, out var key))
            {
                return context;
            }

            var cursorString = context
                .Query
                .GetNode(key)
                .GetConnectionArguments()
                .CursorOrDefault() ?? throw new GraphlessDBOperationException("Expected cursor");

            var cursor = cursorSerializer.Deserialize(cursorString);
            var childCursors = GetChildCursors(cursor);
            var childKeys = context.Query.GetChildNodeKeys(key);
            if (childCursors.Count != childKeys.Count)
            {
                throw new GraphlessDBOperationException("Invalid cursor for query");
            }

            for (var i = 0; i < childCursors.Count; i++)
            {
                context = ApplyCursor(context, childKeys[i], childCursors[i]);
            }

            return context;
        }

        private static ImmutableList<Cursor> GetChildCursors(Cursor cursor)
        {
            return cursor
                .GetChildNodeKeys(cursor.GetRootKey())
                .Select(cursor.GetSubTree)
                .ToImmutableList();
        }

        private GraphExecutionContext ApplyCursor(GraphExecutionContext context, string key, Cursor cursor)
        {
            // If this doesnt support connection / cursors then continue to
            // pass it down
            if (!context.Query.GetNode(key).SupportsConnectionArguments())
            {
                foreach (var childKey in context.Query.GetChildNodeKeys(key))
                {
                    context = ApplyCursor(context, childKey, cursor);
                }

                return context;
            }

            // Apply the cursor
            var cursorString = cursorSerializer.Serialize(cursor);
            context = context with
            {
                Query = context.Query.SetNode(key, context.Query.GetNode(key).WithCursor(cursorString))
            };

            // Apply the child cursor to child nodes
            var childCursors = GetChildCursors(cursor);
            var childKeys = context.Query.GetChildNodeKeys(key);
            for (var i = 0; i < childCursors.Count; i++)
            {
                context = ApplyCursor(context, childKeys[i], childCursors[i]);
            }

            return context;
        }

        private static bool TryGetRootCursorKey(GraphExecutionContext context, out string key)
        {
            return context.Query.TryFind(k => HasCursor(context, k), context.Query.GetRootKey(), out key);
        }

        private static bool HasCursor(GraphExecutionContext context, string key)
        {
            var node = context.Query.GetNode(key);
            return node.SupportsConnectionArguments() && !string.IsNullOrWhiteSpace(node.GetConnectionArguments().CursorOrDefault());
        }

        private GraphExecutionContext EnsureRunnable(GraphExecutionContext context)
        {
            // If there is no root result then it is runnable
            if (context.TryGetRootResult<GraphResult>() == null || !context.GetRootResult<GraphResult>().NeedsMoreData)
            {
                return context;
            }

            // Check for result which needs more data and has more data
            if (context.TryFindResult(key => context.GetResult<GraphResult>(key).NeedsMoreData && context.GetResult<GraphResult>(key).HasMoreData, out var key))
            {
                return context;
            }

            // Check for result which has more child data available
            if (context.TryFindResult(key => !context.GetResult<GraphResult>(key).HasMoreData && HasMoreChildData(context, key), out var keyWithMoreChildData))
            {
                context = context with
                {
                    ResultItems = context.ResultItems.SetItem(
                        keyWithMoreChildData,
                        context.ResultItems[keyWithMoreChildData] with { HasMoreData = true })
                };
            }

            // Check for result which has more data but doesnt need more data???
            if (context.TryFindResult(key => !context.GetResult<GraphResult>(key).NeedsMoreData && context.GetResult<GraphResult>(key).HasMoreData, out var keyWithMoreDataByDoesNotNeed))
            {
                context = context with
                {
                    ResultItems = context.ResultItems.SetItem(
                        keyWithMoreDataByDoesNotNeed,
                        context.ResultItems[keyWithMoreDataByDoesNotNeed] with { NeedsMoreData = true })
                };
            }

            return context;
        }

        private static bool TryGetNextKey(GraphExecutionContext context, out string resultKey)
        {
            var rootResult = context.TryGetRootResult<GraphResult>();
            if (rootResult != null && !rootResult.NeedsMoreData)
            {
                resultKey = string.Empty;
                return false;
            }

            if (context.TryFindResult(key =>
            {
                var anyChildResultsAreMissing = context.TryGetChildResults(key).Where(n => n == null).Any();
                if (anyChildResultsAreMissing)
                {
                    // You have to run the child first if it is missing 
                    // so dont run this one
                    return false;
                }

                var result = context.TryGetResult<GraphResult>(key);
                if (result == null)
                {
                    // If this result is missing then run it
                    return true;
                }

                return result.NeedsMoreData && result.HasMoreData;
            }, out resultKey))
            {
                return true;
            }

            // if (context.TryFindResult(key => HasMoreData(context.GetResult<IGraphResult>(key)), out i))
            // {
            //     return true;
            // }

            return false;
        }

        private bool HasMoreChildData(GraphExecutionContext context, string key)
        {
            var queryExecutor = GetQueryExecutor(context, key);
            var result = queryExecutor.HasMoreChildData(context, key);
            return result;
        }

        private async Task<GraphExecutionContext> ExecuteQueryItemAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            var queryExecutor = GetQueryExecutor(context, key);
            return await queryExecutor.ExecuteAsync(context, key, cancellationToken);
        }

        private IGraphQueryNodeExecutionService GetQueryExecutor(GraphExecutionContext context, string key)
        {
            return context.Query.GetNode(key).Query switch
            {
                NodeByIdQuery => nodeByIdQueryExecutor,
                NodeByNodeQuery => nodeByNodeQueryExecutor,
                NodeOrDefaultByIdQuery => nodeOrDefaultByIdQueryExecutor,
                NodeVersionByIdQuery => nodeVersionByIdQueryExecutor,
                EdgeByIdQuery => edgeByIdQueryExecutor,
                EdgeOrDefaultByIdQuery => edgeOrDefaultByIdQueryExecutor,
                NodeConnectionQuery => nodeConnectionQueryExecutor,
                InToEdgeConnectionQuery => inToEdgeConnectionQueryExecutor,
                InToAllEdgeConnectionQuery => inToAllEdgeConnectionQueryExecutor,
                OutToEdgeConnectionQuery => outToEdgeConnectionQueryExecutor,
                OutToAllEdgeConnectionQuery => outToAllEdgeConnectionQueryExecutor,
                InFromEdgeConnectionQuery => inFromEdgeConnectionQueryExecutor,
                InFromEdgeQuery => inFromEdgeQueryExecutor,
                OutFromEdgeConnectionQuery => outFromEdgeConnectionQueryExecutor,
                OutFromEdgeQuery => outFromEdgeQueryExecutor,
                InAndOutToEdgeConnectionQuery => inAndOutToEdgeConnectionQueryExecutor,
                InAndOutFromEdgeConnectionQuery => inAndOutFromEdgeConnectionQueryExecutor,
                SingleNodeQuery => singleNodeQueryExecutor,
                SingleOrDefaultNodeQuery => singleOrDefaultNodeQueryExecutor,
                FirstNodeQuery => firstNodeQueryExecutor,
                FirstOrDefaultNodeQuery => firstOrDefaultNodeQueryExecutor,
                SingleEdgeQuery => singleEdgeQueryExecutor,
                SingleOrDefaultEdgeQuery => singleOrDefaultEdgeQueryExecutor,
                FirstEdgeQuery => firstEdgeQueryExecutor,
                FirstOrDefaultEdgeQuery => firstOrDefaultEdgeQueryExecutor,
                ZipNodeConnectionQuery => zipNodeConnectionQueryExecutor,
                WhereNodeConnectionQuery => whereNodeConnectionQueryExecutor,
                WhereEdgeConnectionQuery => whereEdgeConnectionQueryExecutor,
                _ => throw new NotSupportedException("Query type not supported"),
            };
        }

        public async Task MutateAsync(
            Func<Task> operation,
            CancellationToken cancellationToken)
        {
            await MutateAsync(async () =>
            {
                await operation();
                return true;
            }, cancellationToken);
        }

        public async Task<T> MutateAsync<T>(
            Func<Task<T>> operation,
            CancellationToken cancellationToken)
        {
            var stopwatch = Stopwatch.StartNew();
            var maxRetryDuration = TimeSpan.FromMinutes(1d);

            // NOTE: Increased this value to account for dynamo stream events where only parts of the data have been updated
            var retryInterval = TimeSpan.FromSeconds(0.2d);
            Exception? lastException = null;
            while (!cancellationToken.IsCancellationRequested)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (stopwatch.Elapsed > maxRetryDuration && !Debugger.IsAttached)
                {
                    throw new TimeoutException("Exceeded maximum allowed time to execute graph mutation operation");
                }

                try
                {
                    return await operation();
                }
                catch (GraphlessDBThroughputExceededException ex)
                {
                    logger.ProvisionedThroughputExceededExceptionCaughtWillRetry(retryInterval.TotalSeconds);
                    // Wait and retry
                    lastException = ex;
                    await Task.Delay(retryInterval, cancellationToken);
                    retryInterval *= _rnd.NextDouble() + 0.75d;
                }
                catch (GraphlessDBConcurrencyException ex)
                {
                    logger.GraphConcurrencyExceptionCaughtWillRetry(retryInterval.TotalSeconds);
                    // Wait and retry
                    lastException = ex;
                    await Task.Delay(retryInterval, cancellationToken);
                    retryInterval *= _rnd.NextDouble() + 0.75d;
                }
                catch (HttpRequestException ex)
                {
                    logger.HttpRequestExceptionCaughtWillRetry(retryInterval.TotalSeconds);
                    // Wait and retry
                    lastException = ex;
                    await Task.Delay(retryInterval, cancellationToken);
                    retryInterval *= _rnd.NextDouble() + 0.75d;
                }
                // NOTE This can occur when you search for nodes which have not been fully commited as a transaction
                // Currently this is too expansive and can cause many retries
                // catch (InvalidOperationException ex)
                // {
                //     _logger.LogWarning("InvalidOperationException caught, will retry. RetryIntervalSeconds = {RetryIntervalSeconds}", retryInterval.TotalSeconds);
                //     // Wait and retry
                //     lastException = ex;
                //     await Task.Delay(retryInterval, cancellationToken);
                //     retryInterval *= 1.5d;
                // }
            }

            if (lastException != null)
            {
                throw lastException;
            }

            throw new GraphlessDBOperationException("Graph mutation failed");
        }

        // public async Task<GraphResult<Connection<RelayEdge<INode>, INode>>> ExecuteAsync(
        //     BackToNodeConnectionQuery query,
        //     ConnectionArguments page,
        //     CancellationToken cancellationToken)
        // {
        //     var childResult = query.ParentQuery switch
        //     {
        //         INodeQuery parentQuery => (GraphResult)await this.ExecuteAsync(parentQuery, cancellationToken),
        //         INodeOrDefaultQuery parentQuery => await this.ExecuteAsync(parentQuery, cancellationToken),
        //         IEdgeQuery parentQuery => await this.ExecuteAsync(parentQuery, cancellationToken),
        //         IEdgeOrDefaultQuery parentQuery => await this.ExecuteAsync(parentQuery, cancellationToken),
        //         INodeConnectionQuery parentQuery => await this.ExecuteAsync(parentQuery, page, cancellationToken),
        //         IEdgeConnectionQuery parentQuery => await this.ExecuteAsync(parentQuery, page, cancellationToken),
        //         IRelayNodeQuery parentQuery => await this.ExecuteToConnectionAsync(parentQuery, cancellationToken),
        //         IRelayNodeOrDefaultQuery parentQuery => await this.ExecuteToConnectionAsync(parentQuery, cancellationToken),
        //         _ => throw new NotSupportedException(),
        //     };

        //     var currentResult = childResult;
        //     while (currentResult.Query.Tag != query.BackToTag)
        //     {
        //         if (currentResult.ParentResult == null)
        //         {
        //             throw new InvalidOperationException("Tag not found");
        //         }

        //         currentResult = currentResult.Query switch
        //         {
        //             //     INodeQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     INodeOrDefaultQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     IEdgeQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     IEdgeOrDefaultQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     INodeConnectionQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     EdgeConnectionByConnectionQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     InToAllEdgeConnectionQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             //     OutToAllEdgeConnectionQuery currentResultQuery => Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             InToEdgeConnectionQuery => BackInToEdgeConnectionQuery(currentResult),
        //             //     OutToEdgeConnectionQuery currentResultQuery =>  Back(currentResultQuery, currentResult, currentResult.ParentResult),
        //             OutFromEdgeConnectionQuery => BackOutFromEdgeConnectionQuery(currentResult),
        //             _ => throw new NotSupportedException(),
        //         };
        //     }

        //     var response = (Connection<RelayEdge<INode>, INode>?)currentResult.Response;
        //     if (response == null)
        //     {
        //         throw new InvalidOperationException();
        //     }

        //     return new GraphResult<Connection<RelayEdge<INode>, INode>>(
        //         query, response, childResult);
        // }

        // private static GraphResult BackInToEdgeConnectionQuery(GraphResult current)
        // {
        //     if (current.ParentResult == null)
        //     {
        //         throw new InvalidOperationException();
        //     }

        //     if (current?.Response is Connection<RelayEdge<IEdge>, IEdge> &&
        //         current?.ParentResult?.Response is Connection<RelayEdge<INode>, INode>)
        //     {
        //         var currentResponse = (Connection<RelayEdge<IEdge>, IEdge>)current.Response;
        //         var edgeInIdSet = currentResponse.Edges.Select(e => e.Node.InId).ToImmutableHashSet();
        //         var parentResponse = (Connection<RelayEdge<INode>, INode>)current.ParentResult.Response;
        //         var backNodes = parentResponse.Edges.Where(e => edgeInIdSet.Contains(e.Node.Id)).ToImmutableList();
        //         var backPageInfo = new PageInfo(
        //             false,
        //             false,
        //             backNodes.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
        //             backNodes.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);
        //         var backConnection = new Connection<RelayEdge<INode>, INode>(backNodes, backPageInfo);
        //         return new GraphResult<Connection<RelayEdge<INode>, INode>>(
        //             current.ParentResult.Query,
        //             backConnection,
        //             current.ParentResult.ParentResult);
        //     }

        //     throw new NotImplementedException();
        // }

        // private static GraphResult BackOutFromEdgeConnectionQuery(GraphResult current)
        // {
        //     if (current.ParentResult == null)
        //     {
        //         throw new InvalidOperationException();
        //     }

        //     if (current?.Response is Connection<RelayEdge<INode>, INode> &&
        //         current?.ParentResult?.Response is Connection<RelayEdge<IEdge>, IEdge>)
        //     {
        //         var currentResponse = (Connection<RelayEdge<INode>, INode>)current.Response;
        //         var nodeIdSet = currentResponse.Edges.Select(e => e.Node.Id).ToImmutableHashSet();
        //         var parentResponse = (Connection<RelayEdge<IEdge>, IEdge>)current.ParentResult.Response;
        //         var backEdges = parentResponse.Edges.Where(e => nodeIdSet.Contains(e.Node.OutId)).ToImmutableList();
        //         var backPageInfo = new PageInfo(
        //             false,
        //             false,
        //             backEdges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
        //             backEdges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);
        //         var backConnection = new Connection<RelayEdge<IEdge>, IEdge>(backEdges, backPageInfo);
        //         return new GraphResult<Connection<RelayEdge<IEdge>, IEdge>>(
        //             current.ParentResult.Query,
        //             backConnection,
        //             current.ParentResult.ParentResult);
        //     }

        //     throw new NotImplementedException();
        // }
    }
}
