/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Query.Datalog.Execution
{
    /// <summary>
    /// Represents a set of variable bindings during datalog query execution.
    /// Each binding row maps variable names to entity instances.
    /// </summary>
    internal sealed class BindingSet
    {
        private readonly ImmutableList<ImmutableDictionary<string, IEntity>> _bindings;

        public static readonly BindingSet Empty = new(ImmutableList<ImmutableDictionary<string, IEntity>>.Empty);

        private BindingSet(ImmutableList<ImmutableDictionary<string, IEntity>> bindings)
        {
            _bindings = bindings;
        }

        /// <summary>
        /// Gets the number of binding rows
        /// </summary>
        public int Count => _bindings.Count;

        /// <summary>
        /// Gets all binding rows
        /// </summary>
        public ImmutableList<ImmutableDictionary<string, IEntity>> Bindings => _bindings;

        /// <summary>
        /// Creates a binding set with a single row containing one variable binding
        /// </summary>
        public static BindingSet Single(string variableName, IEntity value)
        {
            var row = ImmutableDictionary<string, IEntity>.Empty.Add(variableName, value);
            return new BindingSet(ImmutableList.Create(row));
        }

        /// <summary>
        /// Adds a new binding row to this set
        /// </summary>
        public BindingSet Add(ImmutableDictionary<string, IEntity> row)
        {
            return new BindingSet(_bindings.Add(row));
        }

        /// <summary>
        /// Joins this binding set with another binding set
        /// </summary>
        public BindingSet Join(BindingSet other, Core.JoinType joinType = Core.JoinType.Inner)
        {
            if (_bindings.IsEmpty)
                return other;
            if (other._bindings.IsEmpty)
                return this;

            var joined = new List<ImmutableDictionary<string, IEntity>>();

            foreach (var leftRow in _bindings)
            {
                bool foundMatch = false;
                foreach (var rightRow in other._bindings)
                {
                    if (IsCompatible(leftRow, rightRow))
                    {
                        joined.Add(Merge(leftRow, rightRow));
                        foundMatch = true;
                    }
                }

                if (!foundMatch && joinType == Core.JoinType.LeftOuter)
                {
                    joined.Add(leftRow);
                }
            }

            return new BindingSet(joined.ToImmutableList());
        }

        /// <summary>
        /// Combines this binding set with another (union operation)
        /// </summary>
        public BindingSet Union(BindingSet other)
        {
            if (_bindings.IsEmpty)
                return other;
            if (other._bindings.IsEmpty)
                return this;

            return new BindingSet(_bindings.AddRange(other._bindings));
        }

        /// <summary>
        /// Gets all bindings for a specific variable
        /// </summary>
        public IEnumerable<KeyValuePair<string, IEntity>> GetBindings(string variableName)
        {
            foreach (var row in _bindings)
            {
                if (row.TryGetValue(variableName, out var entity))
                {
                    yield return new KeyValuePair<string, IEntity>(variableName, entity);
                }
            }
        }

        /// <summary>
        /// Checks if two binding rows are compatible (no conflicting values for shared variables)
        /// </summary>
        private static bool IsCompatible(
            ImmutableDictionary<string, IEntity> left,
            ImmutableDictionary<string, IEntity> right)
        {
            var commonKeys = left.Keys.Intersect(right.Keys);
            return commonKeys.All(key => EntityEquals(left[key], right[key]));
        }

        /// <summary>
        /// Checks if two entities are equal based on their type and identity
        /// </summary>
        private static bool EntityEquals(IEntity a, IEntity b)
        {
            return (a, b) switch
            {
                (INode nodeA, INode nodeB) => nodeA.Id == nodeB.Id,
                (IEdge edgeA, IEdge edgeB) =>
                    edgeA.InId == edgeB.InId && edgeA.OutId == edgeB.OutId,
                _ => false
            };
        }

        /// <summary>
        /// Merges two compatible binding rows
        /// </summary>
        private static ImmutableDictionary<string, IEntity> Merge(
            ImmutableDictionary<string, IEntity> left,
            ImmutableDictionary<string, IEntity> right)
        {
            var merged = left;
            foreach (var kvp in right)
            {
                if (!merged.ContainsKey(kvp.Key))
                {
                    merged = merged.Add(kvp.Key, kvp.Value);
                }
            }
            return merged;
        }
    }
}
