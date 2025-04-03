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
    public sealed record FluentNodeQuery<TGraph, TNode>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TGraph : IGraph
        where TNode : INode
    {
        public async Task<TNode> GetAsync(
            bool useConsistentRead,
            CancellationToken cancellationToken)
        {
            return await GetAsync(c => c
                .WithConsistentRead(useConsistentRead), cancellationToken);
        }

        public async Task<TNode> GetAsync(
            bool useConsistentRead,
            int intermediateConnectionSize,
            int preFilteredConnectionSize,
            CancellationToken cancellationToken)
        {
            return await GetAsync(c => c
                .WithConsistentRead(useConsistentRead)
                .WithIntermediateConnectionSize(intermediateConnectionSize)
                .WithPreFilteredConnectionSize(preFilteredConnectionSize), cancellationToken);
        }


        public async Task<TNode> GetAsync(
           Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure,
           CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var result = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            var node = result.GetRootResult<NodeResult>().Node?.Node ?? throw new GraphlessDBOperationException("Result node not found");
            return (TNode)node;
        }

        public FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNode, TNodeOutNew> InToEdges<TEdgeNew, TNodeOutNew, TFilter, TOrder>(
            Func<EdgeConnectionOptions<TFilter, TOrder>, EdgeConnectionOptions<TFilter, TOrder>>? configure = null)
            where TEdgeNew : IEdge
            where TNodeOutNew : INode
            where TFilter : class, IEdgeFilter
            where TOrder : class, IEdgeOrder
        {
            var options = configure != null
                ? configure(EdgeConnectionOptions<TFilter, TOrder>.Default)
                : EdgeConnectionOptions<TFilter, TOrder>.Default;

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNode, TNodeOutNew>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InToEdgeConnectionQuery(
                    typeof(TEdgeNew).Name,
                    typeof(TNode).Name,
                    typeof(TNodeOutNew).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNode, TNodeOutNew> InToEdges<TEdgeNew, TNodeOutNew>(
            Func<EdgeConnectionOptions, EdgeConnectionOptions>? configure = null)
            where TEdgeNew : IEdge
            where TNodeOutNew : INode
        {
            var options = EdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNode, TNodeOutNew>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InToEdgeConnectionQuery(
                    typeof(TEdgeNew).Name,
                    typeof(TNode).Name,
                    typeof(TNodeOutNew).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNodeInNew, TNode> OutToEdges<TEdgeNew, TNodeInNew, TFilter, TOrder>(
            Func<EdgeConnectionOptions<TFilter, TOrder>, EdgeConnectionOptions<TFilter, TOrder>>? configure = null)
            where TEdgeNew : IEdge
            where TNodeInNew : INode
            where TFilter : class, IEdgeFilter
            where TOrder : class, IEdgeOrder
        {
            var options = configure != null
                        ? configure(EdgeConnectionOptions<TFilter, TOrder>.Default)
                        : EdgeConnectionOptions<TFilter, TOrder>.Default;

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNodeInNew, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new OutToEdgeConnectionQuery(
                    typeof(TEdgeNew).Name,
                    typeof(TNodeInNew).Name,
                    typeof(TNode).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNodeInNew, TNode> OutToEdges<TEdgeNew, TNodeInNew>(
            Func<EdgeConnectionOptions, EdgeConnectionOptions>? configure = null)
            where TEdgeNew : IEdge
            where TNodeInNew : INode
        {
            var options = EdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, TEdgeNew, TNodeInNew, TNode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new OutToEdgeConnectionQuery(
                    typeof(TEdgeNew).Name,
                    typeof(TNodeInNew).Name,
                    typeof(TNode).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, IEdge, INode, INode> InToEdges(
            Func<EdgeConnectionOptions, EdgeConnectionOptions>? configure = null)
        {
            var options = EdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, IEdge, INode, INode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InToAllEdgeConnectionQuery(
                    typeof(TNode).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeConnectionQuery<TGraph, IEdge, INode, INode> OutToEdges(
            Func<EdgeConnectionOptions, EdgeConnectionOptions>? configure = null)
        {
            var options = EdgeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeConnectionQuery<TGraph, IEdge, INode, INode>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new OutToAllEdgeConnectionQuery(
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
    }
}
