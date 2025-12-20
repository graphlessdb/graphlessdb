/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Query.Datalog.Results
{
    /// <summary>
    /// Result for datalog queries projecting a single variable
    /// </summary>
    public sealed record DatalogConnectionResultSingle(
        ImmutableList<IEntity> Entities,
        string? ChildCursor,
        string Cursor,
        bool NeedsMoreData,
        bool HasMoreData) : GraphResult(ChildCursor, Cursor, NeedsMoreData, HasMoreData);

    /// <summary>
    /// Result for datalog queries projecting multiple variables
    /// Returns bindings containing all projected variables
    /// </summary>
    public sealed record DatalogConnectionResultMulti(
        Connection<RelayEdge<DatalogBindings>, DatalogBindings> Connection,
        string? ChildCursor,
        string Cursor,
        bool NeedsMoreData,
        bool HasMoreData) : GraphResult(ChildCursor, Cursor, NeedsMoreData, HasMoreData);

    /// <summary>
    /// Represents a set of variable bindings in a datalog query result
    /// </summary>
    public sealed record DatalogBindings(
        ImmutableDictionary<string, IEntity> Bindings)
    {
        /// <summary>
        /// Gets the entity bound to the specified variable
        /// </summary>
        public T Get<T>(Core.Variable<T> variable) where T : IEntity
        {
            if (!Bindings.TryGetValue(variable.Name, out var entity))
            {
                throw new System.InvalidOperationException(
                    $"Variable '{variable.Name}' is not bound in this result");
            }

            return (T)entity;
        }

        /// <summary>
        /// Tries to get the entity bound to the specified variable
        /// </summary>
        public bool TryGet<T>(Core.Variable<T> variable, out T? value) where T : IEntity
        {
            if (Bindings.TryGetValue(variable.Name, out var entity))
            {
                value = (T)entity;
                return true;
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Gets the entity bound to the specified variable name
        /// </summary>
        public IEntity Get(string variableName)
        {
            if (!Bindings.TryGetValue(variableName, out var entity))
            {
                throw new System.InvalidOperationException(
                    $"Variable '{variableName}' is not bound in this result");
            }

            return entity;
        }

        /// <summary>
        /// Tries to get the entity bound to the specified variable name
        /// </summary>
        public bool TryGet(string variableName, out IEntity? value)
        {
            return Bindings.TryGetValue(variableName, out value);
        }
    }
}
