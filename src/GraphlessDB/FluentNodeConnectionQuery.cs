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
    public sealed record FluentNodeConnectionQuery<TGraph, TNode> : IFluentQuery
        where TNode : INode
        where TGraph : IGraph
    {
        public FluentNodeConnectionQuery(
            IGraphQueryExecutionService graphQueryService,
            ImmutableTree<string, GraphQueryNode> query,
            string key)
        {
            GraphQueryService = graphQueryService;
            Query = query;
            Key = key;
        }

        public IGraphQueryExecutionService GraphQueryService { get; }

        public ImmutableTree<string, GraphQueryNode> Query { get; }

        public string Key { get; }

        public async Task<ImmutableList<TNode>> GetEntitiesAsync(
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

        public async Task<Connection<RelayEdge<TNode>, TNode>> GetAsync(
            bool useConsistentRead,
            ConnectionArguments connectionArguments,
            CancellationToken cancellationToken)
        {
            return await GetAsync(c => c
                .WithConsistentRead(useConsistentRead)
                .WithConnectionArguments(connectionArguments),
                cancellationToken);
        }

        public async Task<Connection<RelayEdge<TNode>, TNode>> GetAsync(
            bool useConsistentRead,
            ConnectionArguments connectionArguments,
            int intermediateConnectionSize,
            int preFilteredConnectionSize,
            CancellationToken cancellationToken)
        {
            return await GetAsync(c => c
                .WithConsistentRead(useConsistentRead)
                .WithConnectionArguments(connectionArguments)
                .WithIntermediateConnectionSize(intermediateConnectionSize)
                .WithPreFilteredConnectionSize(preFilteredConnectionSize),
                cancellationToken);
        }

        public async Task<Connection<RelayEdge<TNode>, TNode>> GetAsync(
            Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure,
            CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var resultContext = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            var result = resultContext
                .GetRootResult<NodeConnectionResult>()
                .Connection
                .AsType<TNode>();

            return result with
            {
                PageInfo = result.PageInfo with
                {
                    HasNextPage = resultContext.ResultItems.Values.Where(v => v.IsConnection() && v.GetPageInfo().HasNextPage).Any(),
                    HasPreviousPage = resultContext.ResultItems.Values.Where(v => v.IsConnection() && v.GetPageInfo().HasPreviousPage).Any(),
                }
            };
        }

        public FluentNodeQuery<TGraph, TNode> First(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new FirstNodeQuery(tag))),
                key);
        }

#pragma warning disable CA1720
        public FluentNodeQuery<TGraph, TNode> Single(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new SingleNodeQuery(tag))),
                key);
        }
#pragma warning restore CA1720

        public FluentNodeOrDefaultQuery<TGraph, TNode> FirstOrDefault(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeOrDefaultQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new FirstOrDefaultNodeQuery(tag))),
                key);
        }

        public FluentNodeOrDefaultQuery<TGraph, TNode> SingleOrDefault(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeOrDefaultQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new SingleOrDefaultNodeQuery(tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNode, TNodeOut> InToEdges<TEdge, TNodeOut>(
            Func<ToEdgeConnectionOptions, ToEdgeConnectionOptions>? configure = null)
            where TEdge : IEdge
            where TNodeOut : INode
        {
            var options = ToEdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdge, TNode, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InToEdgeConnectionQuery(
                    typeof(TEdge).Name,
                    typeof(TNode).Name,
                    typeof(TNodeOut).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNode> OutToEdges<TEdge, TNodeIn>(
            Func<ToEdgeConnectionOptions, ToEdgeConnectionOptions>? configure = null)
            where TEdge : IEdge
            where TNodeIn : INode
        {
            var options = ToEdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdge, TNodeIn, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new OutToEdgeConnectionQuery(
                    typeof(TEdge).Name,
                    typeof(TNodeIn).Name,
                    typeof(TNode).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdge, TNode, TNode> InAndOutToEdges<TEdge>(
            Func<ToEdgeConnectionOptions, ToEdgeConnectionOptions>? configure = null)
            where TEdge : IEdge
        {
            var options = ToEdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdge, TNode, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InAndOutToEdgeConnectionQuery(
                    typeof(TEdge).Name,
                    typeof(TNode).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> Zip(FluentNodeConnectionQuery<TGraph, TNode> other, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();

            var node = new GraphQueryNode(
                new ZipNodeConnectionQuery(other.Query, ConnectionArguments.Default, 25, tag));

            var query = Query
                .AddParentNode(Key, key, node)
                .AddSubTree(key, other.Query);

            return new FluentNodeConnectionQuery<TGraph, TNode>(
                GraphQueryService,
                query,
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> WhereAsync(
            Func<WhereRelayNodeContext<TGraph, TNode>, Task<bool>> predicate, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new WhereNodeConnectionQuery(
                    (ctx) => predicate(new WhereRelayNodeContext<TGraph, TNode>(
                        new FluentGraphQuery<TGraph>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        new FluentNodeQuery<TGraph, TNode>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        ctx.Item.AsType<TNode>(),
                        ctx.UseConsistentRead,
                        ctx.CancellationToken)),
                    ConnectionArguments.Default,
                    25,
                    false,
                    tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> WhereNodeAsync(
            Func<WhereNodeContext<TGraph, TNode>, Task<bool>> predicate, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new WhereNodeConnectionQuery(
                    (ctx) => predicate(new WhereNodeContext<TGraph, TNode>(
                        new FluentGraphQuery<TGraph>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        new FluentNodeQuery<TGraph, TNode>(GraphQueryService, ImmutableTree<string, GraphQueryNode>.Empty, string.Empty),
                        ctx.Item.AsType<TNode>().Node,
                        ctx.UseConsistentRead,
                        ctx.CancellationToken)),
                    ConnectionArguments.Default,
                    25,
                    false,
                    tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> Where(
            Func<WhereRelayNodeContext<TGraph, TNode>, bool> predicate, string? tag = null)
        {
            return WhereAsync(v => Task.FromResult(predicate(v)), tag);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> WhereNode(
            Func<WhereNodeContext<TGraph, TNode>, bool> predicate, string? tag = null)
        {
            return WhereNodeAsync(v => Task.FromResult(predicate(v)), tag);
        }

        public async Task<bool> AnyAsync(
            bool useConsistentRead,
            CancellationToken cancellationToken)
        {
            var item = await FirstOrDefault()
                .GetAsync(useConsistentRead, cancellationToken);

            return item != null;
        }
    }
}
