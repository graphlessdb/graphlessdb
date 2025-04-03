/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;

namespace GraphlessDB
{
    public sealed record FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TGraph : IGraph
        where TEdge : IEdge
        where TNodeIn : INode
        where TNodeOut : INode
    {
        public async Task<ImmutableList<TEdge>> GetEntitiesAsync(
            bool useConsistentRead,
            ConnectionArguments connectionArguments,
            CancellationToken cancellationToken)
        {
            var connection = await GetAsync(c => c
                .WithConsistentRead(useConsistentRead)
                .WithConnectionArguments(connectionArguments),
                cancellationToken);

            return connection.ToImmutableNodeList();
        }

        public async Task<Connection<RelayEdge<TEdge>, TEdge>> GetAsync(
            bool useConsistentRead,
            ConnectionArguments connectionArguments,
            CancellationToken cancellationToken)
        {
            return await GetAsync(c => c
                .WithConsistentRead(useConsistentRead)
                .WithConnectionArguments(connectionArguments),
                cancellationToken);
        }

        public async Task<bool> AnyAsync(
            bool useConsistentRead,
            CancellationToken cancellationToken)
        {
            var item = await FirstOrDefault()
                .GetAsync(useConsistentRead, cancellationToken);

            return item != null;
        }

        public async Task<Connection<RelayEdge<TEdge>, TEdge>> GetAsync(
            Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure,
            CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var resultContext = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            var result = resultContext
                .GetRootResult<EdgeConnectionResult>()
                .Connection
                .AsType<TEdge>();

            return result with
            {
                PageInfo = result.PageInfo with
                {
                    HasNextPage = resultContext.ResultItems.Values.Where(v => v.IsConnection() && v.GetPageInfo().HasNextPage).Any(),
                    HasPreviousPage = resultContext.ResultItems.Values.Where(v => v.IsConnection() && v.GetPageInfo().HasPreviousPage).Any(),
                }
            };
        }

#pragma warning disable CA1720
        public FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut> Single(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new SingleEdgeQuery(tag))),
                key);
        }
#pragma warning restore CA1720

        public FluentEdgeOrDefaultQuery<TGraph, TEdge, TNodeIn, TNodeOut> SingleOrDefault(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeOrDefaultQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new SingleOrDefaultEdgeQuery(tag))),
                key);
        }

        public FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut> First(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new FirstEdgeQuery(tag))),
                key);
        }

        public FluentEdgeOrDefaultQuery<TGraph, TEdge, TNodeIn, TNodeOut> FirstOrDefault(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeOrDefaultQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new FirstOrDefaultEdgeQuery(tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNodeIn> InFromEdges(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNodeIn>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InFromEdgeConnectionQuery(null, false, tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNodeOut> OutFromEdges(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new OutFromEdgeConnectionQuery(null, false, tag))),
                key);
        }

        // TODO NodeConnectionOptions contains ordering which is not possible
        public FluentNodeConnectionQuery<TGraph, TNodeOut> InAndOutFromEdges(Func<NodeConnectionOptions, NodeConnectionOptions>? configure = null)
        {
            var options = NodeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InAndOutFromEdgeConnectionQuery(
                    options.Filter,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut> WhereAsync(
            Func<WhereRelayEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>, Task<bool>> predicate, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new WhereEdgeConnectionQuery(
                    (ctx) => predicate(new WhereRelayEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>(
                        new FluentGraphQuery<TGraph>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        new FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        ctx.Item.AsType<TEdge>(),
                        ctx.UseConsistentRead,
                        ctx.CancellationToken)),
                    ConnectionArguments.Default,
                    25,
                    false,
                    tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut> WhereEdgeAsync(
            Func<WhereEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>, Task<bool>> predicate, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new WhereEdgeConnectionQuery(
                    (ctx) => predicate(new WhereEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>(
                        new FluentGraphQuery<TGraph>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        new FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        ctx.Item.AsType<TEdge>().Node,
                        ctx.UseConsistentRead,
                        ctx.CancellationToken)),
                    ConnectionArguments.Default,
                    25,
                    false,
                    tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut> Where(
             Func<WhereRelayEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>, bool> predicate, string? tag = null)
        {
            return WhereAsync(v => Task.FromResult(predicate(v)), tag);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNodeOut> WhereEdge(
            Func<WhereEdgeContext<TGraph, TEdge, TNodeIn, TNodeOut>, bool> predicate, string? tag = null)
        {
            return WhereEdgeAsync(v => Task.FromResult(predicate(v)), tag);
        }
    }
}