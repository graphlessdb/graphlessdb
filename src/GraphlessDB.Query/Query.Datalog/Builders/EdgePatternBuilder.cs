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

namespace GraphlessDB.Query.Datalog.Builders
{
    /// <summary>
    /// Builder for configuring edge patterns
    /// </summary>
    public sealed class EdgePatternBuilder<TEdge> where TEdge : IEdge
    {
        private readonly Variable<INode> _from;
        private readonly Variable<INode> _to;
        private readonly List<PropertyConstraint> _constraints = new();

        internal EdgePatternBuilder(Variable<INode> from, Variable<INode> to)
        {
            _from = from;
            _to = to;
        }

        /// <summary>
        /// Adds a property filter constraint for the edge
        /// </summary>
        public EdgePatternBuilder<TEdge> Where(string propertyName, IValueFilter filter)
        {
            _constraints.Add(new PropertyConstraint(propertyName, filter));
            return this;
        }

        /// <summary>
        /// Adds a property equals constraint for the edge
        /// </summary>
        public EdgePatternBuilder<TEdge> WhereEquals(string propertyName, string value)
        {
            return Where(propertyName, new StringFilter { Eq = value });
        }

        /// <summary>
        /// Adds a property BeginsWith constraint for the edge
        /// </summary>
        public EdgePatternBuilder<TEdge> WhereBeginsWith(string propertyName, string value)
        {
            return Where(propertyName, new StringFilter { BeginsWith = value });
        }

        internal EdgePattern Build()
        {
            return new EdgePattern(
                Guid.NewGuid().ToString(),
                _from,
                typeof(TEdge).Name,
                _to,
                _constraints.ToImmutableList());
        }
    }
}
