/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Collections;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;

namespace GraphlessDB
{
    public sealed record FluentNodeOrDefaultFromRelayNodeOrDefaultQuery<TGraph, TNode>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TGraph : IGraph
        where TNode : INode
    {
        public async Task<TNode?> GetAsync(CancellationToken cancellationToken)
        {
            return await GetAsync(c => c, cancellationToken);
        }

        public async Task<TNode?> GetAsync(Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure, CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var result = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            return (TNode?)result.GetRootResult<NodeResult>().Node?.Node;
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
    }
}
