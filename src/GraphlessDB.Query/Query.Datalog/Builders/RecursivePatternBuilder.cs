/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Query.Datalog.Core;

namespace GraphlessDB.Query.Datalog.Builders
{
    /// <summary>
    /// Builder for configuring recursive/transitive patterns
    /// </summary>
    public sealed class RecursivePatternBuilder<TEdge, TNode>
        where TEdge : IEdge
        where TNode : INode
    {
        private readonly Variable<TNode> _start;
        private readonly Variable<TNode> _end;
        private int _minDepth = 1;
        private int _maxDepth = RecursivePattern.UnboundedDepth;
        private RecursionMode _mode = RecursionMode.BreadthFirst;
        private Func<EdgePatternBuilder<TEdge>, EdgePatternBuilder<TEdge>>? _edgeConfigure;

        internal RecursivePatternBuilder(Variable<TNode> start, Variable<TNode> end)
        {
            _start = start;
            _end = end;
        }

        /// <summary>
        /// Specifies the edge pattern to follow during recursion
        /// </summary>
        public RecursivePatternBuilder<TEdge, TNode> Via(
            Func<EdgePatternBuilder<TEdge>, EdgePatternBuilder<TEdge>> configure)
        {
            _edgeConfigure = configure;
            return this;
        }

        /// <summary>
        /// Sets the minimum depth for results (default: 1)
        /// </summary>
        public RecursivePatternBuilder<TEdge, TNode> MinDepth(int depth)
        {
            _minDepth = depth;
            return this;
        }

        /// <summary>
        /// Sets the maximum depth for traversal
        /// </summary>
        public RecursivePatternBuilder<TEdge, TNode> MaxDepth(int depth)
        {
            _maxDepth = depth;
            return this;
        }

        /// <summary>
        /// Allows unbounded recursion (up to safety limit)
        /// </summary>
        public RecursivePatternBuilder<TEdge, TNode> Unbounded()
        {
            _maxDepth = RecursivePattern.UnboundedDepth;
            return this;
        }

        /// <summary>
        /// Uses breadth-first search (default)
        /// </summary>
        public RecursivePatternBuilder<TEdge, TNode> BreadthFirst()
        {
            _mode = RecursionMode.BreadthFirst;
            return this;
        }

        /// <summary>
        /// Uses depth-first search
        /// </summary>
        public RecursivePatternBuilder<TEdge, TNode> DepthFirst()
        {
            _mode = RecursionMode.DepthFirst;
            return this;
        }

        internal RecursivePattern Build()
        {
            var edgeBuilder = new EdgePatternBuilder<TEdge>(
                (Variable<INode>)(object)_start,
                (Variable<INode>)(object)_end);
            edgeBuilder = _edgeConfigure?.Invoke(edgeBuilder) ?? edgeBuilder;
            var edgePattern = edgeBuilder.Build();

            return new RecursivePattern(
                Guid.NewGuid().ToString(),
                (Variable<INode>)(object)_start,
                (Variable<INode>)(object)_end,
                edgePattern,
                _minDepth,
                _maxDepth,
                _mode);
        }
    }
}
