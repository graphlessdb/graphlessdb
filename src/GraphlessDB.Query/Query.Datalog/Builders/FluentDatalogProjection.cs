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
using GraphlessDB.Domain.Graph;
using GraphlessDB.Query.Datalog.Core;
using GraphlessDB.Query.Datalog.Results;
using GraphlessDB.Query.Services;

namespace GraphlessDB.Query.Datalog.Builders
{
    /// <summary>
    /// Handles projection and execution of datalog queries
    /// </summary>
    public sealed class FluentDatalogProjection<TGraph, TEntity>
        where TGraph : IGraph
        where TEntity : IEntity
    {
        private readonly IGraphQueryExecutionService _executionService;
        private readonly ImmutableList<Pattern> _patterns;
        private readonly ImmutableList<RecursivePattern> _recursivePatterns;
        private readonly ImmutableList<VariableProjection> _projections;
        private readonly bool _consistentRead;
        private readonly string? _tag;

        internal FluentDatalogProjection(
            IGraphQueryExecutionService executionService,
            ImmutableList<Pattern> patterns,
            ImmutableList<RecursivePattern> recursivePatterns,
            ImmutableList<VariableProjection> projections,
            bool consistentRead,
            string? tag)
        {
            _executionService = executionService;
            _patterns = patterns;
            _recursivePatterns = recursivePatterns;
            _projections = projections;
            _consistentRead = consistentRead;
            _tag = tag;
        }

        /// <summary>
        /// Applies ordering to node projections
        /// </summary>
        public FluentDatalogProjection<TGraph, TEntity> OrderBy<TNode>(INodeOrder order)
            where TNode : INode
        {
            var updatedProjection = _projections[0] with { Order = order };
            return new FluentDatalogProjection<TGraph, TEntity>(
                _executionService,
                _patterns,
                _recursivePatterns,
                ImmutableList.Create(updatedProjection),
                _consistentRead,
                _tag);
        }

        /// <summary>
        /// Executes the query and returns results
        /// </summary>
        public async Task<ImmutableList<TEntity>> GetAsync(
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var projectionSpec = new ProjectionSpec(
                _projections,
                ConnectionArguments.FirstMax,
                1000);

            var datalogQuery = new DatalogQuery(
                _patterns,
                _recursivePatterns,
                projectionSpec,
                null,
                consistentRead,
                _tag);

            // Create query tree and execute
            var key = Guid.NewGuid().ToString();
            var tree = ImmutableTree<string, GraphQueryNode>.Empty
                .AddNode(key, new GraphQueryNode(datalogQuery));

            var context = await _executionService.GetAsync(tree, cancellationToken);
            var result = context.GetResult<DatalogConnectionResultSingle>(key);

            return result.Entities.Cast<TEntity>().ToImmutableList();
        }
    }
}
