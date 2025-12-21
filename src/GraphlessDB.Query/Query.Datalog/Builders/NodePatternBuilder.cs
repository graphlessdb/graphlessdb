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
using System.Linq;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Query.Datalog.Core;

namespace GraphlessDB.Query.Datalog.Builders
{
    /// <summary>
    /// Builder for configuring node patterns with property constraints
    /// </summary>
    public sealed class NodePatternBuilder<TNode> where TNode : INode
    {
        private readonly Variable<TNode> _variable;
        private readonly List<PropertyConstraint> _constraints = new();

        internal NodePatternBuilder(Variable<TNode> variable)
        {
            _variable = variable;
        }

        /// <summary>
        /// Adds a property filter constraint
        /// </summary>
        public NodePatternBuilder<TNode> Where(string propertyName, IValueFilter filter)
        {
            _constraints.Add(new PropertyConstraint(propertyName, filter));
            return this;
        }

        /// <summary>
        /// Adds a property equals constraint
        /// </summary>
        public NodePatternBuilder<TNode> WhereEquals(string propertyName, string value)
        {
            return Where(propertyName, new StringFilter { Eq = value });
        }

        /// <summary>
        /// Adds a property IN constraint (matches any of the specified values)
        /// </summary>
        public NodePatternBuilder<TNode> WhereIn(string propertyName, params string[] values)
        {
            return Where(propertyName, new StringFilter { In = values.ToArray() });
        }

        /// <summary>
        /// Adds a property BeginsWith constraint
        /// </summary>
        public NodePatternBuilder<TNode> WhereBeginsWith(string propertyName, string value)
        {
            return Where(propertyName, new StringFilter { BeginsWith = value });
        }

        internal NodePattern Build()
        {
            // Create a Variable<INode> with the same name as the original variable
            // We can't directly cast Variable<TNode> to Variable<INode> due to generic invariance
            var nodeVariable = new Variable<INode>(_variable.Name);
            
            return new NodePattern(
                Guid.NewGuid().ToString(),
                nodeVariable,
                typeof(TNode).Name,
                _constraints.ToImmutableList());
        }
    }
}
