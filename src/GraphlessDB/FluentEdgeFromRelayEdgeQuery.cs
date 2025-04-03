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
    public sealed record FluentEdgeFromRelayEdgeQuery(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
    {
        public async Task<IEdge> GetAsync(Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure, CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var result = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            return result
                .GetRootResult<EdgeResult>()
                .Edge
                ?.Node ?? throw new GraphlessDBOperationException("Result node not found");
        }
    }

    public sealed record FluentEdgeFromRelayEdgeQuery<TEdge>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TEdge : IEdge
    {
        public async Task<TEdge> GetAsync(Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure, CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var result = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            var edge = result
                .GetRootResult<EdgeResult>()
                .Edge
                ?.Node ?? throw new GraphlessDBOperationException("Result node not found");

            return (TEdge)edge;
        }
    }
}
