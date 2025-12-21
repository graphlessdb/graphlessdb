/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Query.Datalog.Core;
using GraphlessDB.Query.Services;

namespace GraphlessDB.Query.Datalog.Builders
{
    /// <summary>
    /// Main entry point for building datalog queries
    /// </summary>
    public sealed class FluentDatalogQuery<TGraph> where TGraph : IGraph
    {
        private readonly IGraphQueryExecutionService _executionService;
        private readonly ImmutableList<Pattern> _patterns;
        private readonly ImmutableList<RecursivePattern> _recursivePatterns;
        private readonly Dictionary<string, object> _variables;
        private readonly bool _consistentRead;
        private readonly string? _tag;

        public FluentDatalogQuery(
            IGraphQueryExecutionService executionService,
            ImmutableList<Pattern>? patterns = null,
            ImmutableList<RecursivePattern>? recursivePatterns = null,
            Dictionary<string, object>? variables = null,
            bool consistentRead = false,
            string? tag = null)
        {
            _executionService = executionService;
            _patterns = patterns ?? ImmutableList<Pattern>.Empty;
            _recursivePatterns = recursivePatterns ?? ImmutableList<RecursivePattern>.Empty;
            _variables = variables ?? new Dictionary<string, object>();
            _consistentRead = consistentRead;
            _tag = tag;
        }

        /// <summary>
        /// Creates a new variable for use in patterns
        /// </summary>
        public FluentDatalogQuery<TGraph> Var<TEntity>(out Variable<TEntity> variable, string? name = null)
            where TEntity : IEntity
        {
            variable = Core.Var.Create<TEntity>(name);
            _variables[variable.Name] = variable;
            return this;
        }

        /// <summary>
        /// Adds a node pattern to the query
        /// </summary>
        public FluentDatalogQuery<TGraph> Match<TNode>(
            Variable<TNode> variable,
            Func<NodePatternBuilder<TNode>, NodePatternBuilder<TNode>>? configure = null)
            where TNode : INode
        {
            var builder = new NodePatternBuilder<TNode>(variable);
            builder = configure?.Invoke(builder) ?? builder;
            var pattern = builder.Build();

            return new FluentDatalogQuery<TGraph>(
                _executionService,
                _patterns.Add(pattern),
                _recursivePatterns,
                _variables,
                _consistentRead,
                _tag);
        }

        /// <summary>
        /// Adds an edge pattern to the query
        /// </summary>
        public FluentDatalogQuery<TGraph> Edge<TEdge, TNodeIn, TNodeOut>(
            Variable<TNodeIn> from,
            Variable<TNodeOut> to,
            Func<EdgePatternBuilder<TEdge>, EdgePatternBuilder<TEdge>>? configure = null)
            where TEdge : IEdge
            where TNodeIn : INode
            where TNodeOut : INode
        {
            // Create Variable<INode> instances with the same names as the original variables
            var fromNode = new Variable<INode>(from.Name);
            var toNode = new Variable<INode>(to.Name);
            
            var builder = new EdgePatternBuilder<TEdge>(fromNode, toNode);
            builder = configure?.Invoke(builder) ?? builder;
            var pattern = builder.Build();

            return new FluentDatalogQuery<TGraph>(
                _executionService,
                _patterns.Add(pattern),
                _recursivePatterns,
                _variables,
                _consistentRead,
                _tag);
        }

        /// <summary>
        /// Adds a recursive pattern to the query
        /// </summary>
        public FluentDatalogQuery<TGraph> Recursive<TEdge, TNode>(
            Variable<TNode> start,
            Variable<TNode> end,
            Func<RecursivePatternBuilder<TEdge, TNode>, RecursivePatternBuilder<TEdge, TNode>> configure)
            where TEdge : IEdge
            where TNode : INode
        {
            var builder = new RecursivePatternBuilder<TEdge, TNode>(start, end);
            builder = configure(builder);
            var pattern = builder.Build();

            return new FluentDatalogQuery<TGraph>(
                _executionService,
                _patterns,
                _recursivePatterns.Add(pattern),
                _variables,
                _consistentRead,
                _tag);
        }

        /// <summary>
        /// Selects a single variable to return from the query
        /// </summary>
        public FluentDatalogProjection<TGraph, TEntity> Select<TEntity>(Variable<TEntity> variable)
            where TEntity : IEntity
        {
            var projection = new VariableProjection(variable.Name, typeof(TEntity), null);

            return new FluentDatalogProjection<TGraph, TEntity>(
                _executionService,
                _patterns,
                _recursivePatterns,
                ImmutableList.Create(projection),
                _consistentRead,
                _tag);
        }
    }
}
