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
using GraphlessDB.Collections;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;

namespace GraphlessDB
{
    public sealed record FluentGraphQuery<TGraph>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TGraph : IGraph
    {
        public FluentNodeQuery<TGraph, INode> Node(string id, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, INode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeByIdQuery(id, false, tag))),
                key);
        }

        public FluentNodeQuery<TGraph, TNode> Node<TNode>(string id, string? tag = null)
            where TNode : INode
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeByIdQuery(id, false, tag))),
                key);
        }

        public FluentNodeQuery<TGraph, TNode> Node<TNode>(TNode node, string? tag = null)
            where TNode : INode
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeByNodeQuery(node, false, tag))),
                key);
        }

        public FluentNodeOrDefaultQuery<TGraph, INode> NodeOrDefault(string id, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeOrDefaultQuery<TGraph, INode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeOrDefaultByIdQuery(id, false, tag))),
                key);
        }

        public FluentNodeOrDefaultQuery<TGraph, TNode> NodeOrDefault<TNode>(string id, string? tag = null)
            where TNode : INode
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeOrDefaultQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeOrDefaultByIdQuery(id, false, tag))),
                key);
        }

        public FluentNodeQuery<TGraph, TNode> NodeVersion<TNode>(TNode node, int version, string? tag = null)
            where TNode : INode
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeVersionByIdQuery(node.Id, version, false, tag))),
                key);
        }

        public FluentNodeQuery<TGraph, TNode> NodeVersion<TNode>(string id, int version, string? tag = null)
            where TNode : INode
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeVersionByIdQuery(id, version, false, tag))),
                key);
        }

        public FluentNodeQuery<TGraph, INode> NodeVersion(string id, int version, string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, INode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeVersionByIdQuery(id, version, false, tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> Nodes<TNode>(
            Func<NodeConnectionOptions, NodeConnectionOptions>? configure = null)
            where TNode : INode
        {
            var options = NodeConnectionOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeConnectionQuery(
                    typeof(TNode).Name,
                    options.Filter,
                    options.Order,
                    ConnectionArguments.Default,
                    options.PageSize,
                    false,
                    options.Tag))),
                key);
        }

        public FluentNodeConnectionQuery<TGraph, TNode> Nodes<TNode, TNodeFilter, TNodeOrder>(
            Func<NodeConnectionOptions<TNodeFilter, TNodeOrder>, NodeConnectionOptions<TNodeFilter, TNodeOrder>>? configure = null)
            where TNode : INode
            where TNodeFilter : class, INodeFilter
            where TNodeOrder : class, INodeOrder
        {
            var options = configure != null
                 ? configure(NodeConnectionOptions<TNodeFilter, TNodeOrder>.Empty)
                 : null;

            var key = Guid.NewGuid().ToString();
            return new FluentNodeConnectionQuery<TGraph, TNode>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new NodeConnectionQuery(
                    typeof(TNode).Name,
                    options?.Filter,
                    options?.Order,
                    ConnectionArguments.Default,
                    25,
                    false,
                    options?.Tag))),
                key);
        }

        public FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut> Edge<TEdge, TNodeIn, TNodeOut>(
            TEdge edge, string? tag = null)
            where TEdge : IEdge
            where TNodeIn : INode
            where TNodeOut : INode
        {
            var key = Guid.NewGuid().ToString();
            return new FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new EdgeByIdQuery(
                    typeof(TEdge).Name,
                    edge.InId,
                    edge.OutId,
                    false,
                    tag))),
                key);
        }

        public FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut> Edge<TEdge, TNodeIn, TNodeOut>(
            string inId,
            string outId,
            Func<EdgeOptions, EdgeOptions>? configure = null)
            where TEdge : IEdge
            where TNodeIn : INode
            where TNodeOut : INode
        {
            var options = EdgeOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new EdgeByIdQuery(
                    typeof(TEdge).Name,
                    inId,
                    outId,
                    false,
                    options.Tag))),
                key);
        }

        public FluentEdgeOrDefaultQuery<TGraph, TEdge, TNodeIn, TNodeOut> EdgeOrDefault<TEdge, TNodeIn, TNodeOut>(
            string inId,
            string outId,
            Func<EdgeOptions, EdgeOptions>? configure = null)
            where TEdge : IEdge
            where TNodeIn : INode
            where TNodeOut : INode
        {
            var options = EdgeOptions.Default;
            if (configure != null)
            {
                options = configure(options);
            }

            var key = Guid.NewGuid().ToString();
            return new FluentEdgeOrDefaultQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
                GraphQueryService,
                Query.AddNode(key, new GraphQueryNode(new EdgeOrDefaultByIdQuery(
                    typeof(TEdge).Name,
                    inId,
                    outId,
                    false,
                    options.Tag))),
                key);
        }

        public FluentPut Put(PutQuery query)
        {
            return new FluentPut(
                GraphQueryService,
                query);
        }

        public FluentPut Put<T>(T entity)
            where T : IEntity
        {
            return new FluentPut(
                GraphQueryService,
                new PutQuery([entity], [], [], [], false));
        }

        public FluentPut Put(params IEntity[] entities)
        {
            return new FluentPut(
                GraphQueryService,
                new PutQuery([.. entities], [], [], [], false));
        }

        public FluentPut Put<T>(ImmutableList<T> entities)
            where T : IEntity
        {
            return new FluentPut(
                GraphQueryService,
                new PutQuery(entities.Cast<IEntity>().ToImmutableList(), [], [], [], false));
        }

        public FluentClear Clear()
        {
            return new FluentClear(GraphQueryService);
        }
    }
}
