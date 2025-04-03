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
    public sealed record FluentNodeFromRelayNodeQuery<TNode>(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        string Key) : IFluentQuery
        where TNode : INode
    {
        public async Task<TNode> GetAsync(bool useConsistentRead, CancellationToken cancellationToken)
        {
            return await GetAsync(c => c.WithConsistentRead(useConsistentRead), cancellationToken);
        }

        public async Task<TNode> GetAsync(Func<ImmutableTree<string, GraphQueryNode>, ImmutableTree<string, GraphQueryNode>> configure, CancellationToken cancellationToken)
        {
            var updatedQuery = configure(Query);
            var result = await GraphQueryService.GetAsync(updatedQuery, cancellationToken);
            var node = result.GetRootResult<NodeResult>().Node?.Node ?? throw new GraphlessDBOperationException("Result node not found");
            return (TNode)node;
        }
    }
}
