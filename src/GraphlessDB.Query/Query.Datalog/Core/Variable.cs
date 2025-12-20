/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Query.Datalog.Core
{
    /// <summary>
    /// Represents a strongly-typed variable in a datalog query pattern.
    /// Variables are used to bind entities during pattern matching.
    /// </summary>
    /// <typeparam name="TEntity">The type of entity this variable can bind to (INode or IEdge)</typeparam>
    public sealed class Variable<TEntity> where TEntity : IEntity
    {
        /// <summary>
        /// Gets the unique name of this variable
        /// </summary>
        internal string Name { get; }

        /// <summary>
        /// Gets the entity type this variable represents
        /// </summary>
        internal Type EntityType { get; }

        internal Variable(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            EntityType = typeof(TEntity);
        }

        public override string ToString() => $"?{Name}";

        public override bool Equals(object? obj)
        {
            return obj is Variable<TEntity> other && Name == other.Name;
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
    }

    /// <summary>
    /// Factory for creating strongly-typed variables
    /// </summary>
    public static class Var
    {
        private static int _counter;

        /// <summary>
        /// Creates a new variable with the specified or auto-generated name
        /// </summary>
        /// <typeparam name="TEntity">The entity type this variable represents</typeparam>
        /// <param name="name">Optional name for the variable. If null, an auto-generated name will be used.</param>
        /// <returns>A new variable instance</returns>
        public static Variable<TEntity> Create<TEntity>(string? name = null)
            where TEntity : IEntity
        {
            name ??= $"var_{Interlocked.Increment(ref _counter)}";
            return new Variable<TEntity>(name);
        }
    }
}
