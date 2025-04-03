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
    public sealed record FluentEdgeQuery<TGraph, TEdge, TNodeIn, TNodeOut>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TGraph : IGraph
        where TEdge : IEdge
        where TNodeIn : INode
        where TNodeOut : INode
    {
        public async Task<TEdge> GetAsync(
            bool useConsistentRead, CancellationToken cancellationToken)
        {
            return await GetAsync(c => c.WithConsistentRead(useConsistentRead), cancellationToken);
        }

        public async Task<TEdge> GetAsync(
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

        public async Task<TEdge> GetAsync(
            Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure,
            CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var result = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            var edge = result.GetRootResult<EdgeResult>().Edge?.Node ?? throw new GraphlessDBOperationException("Result node not found");
            return (TEdge)edge;
        }

        public FluentNodeQuery<TGraph, TNodeIn> InFromEdge(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNodeIn>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new InFromEdgeQuery(false, tag))),
                key);
        }

        public FluentNodeQuery<TGraph, TNodeOut> OutFromEdge(string? tag = null)
        {
            var key = Guid.NewGuid().ToString();
            return new FluentNodeQuery<TGraph, TNodeOut>(
                GraphQueryService,
                Query.AddParentNode(Key, key, new GraphQueryNode(new OutFromEdgeQuery(false, tag))),
                key);
        }
    }
}