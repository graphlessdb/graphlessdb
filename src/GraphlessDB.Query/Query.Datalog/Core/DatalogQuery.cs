/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.Query.Datalog.Core
{
    /// <summary>
    /// Represents a datalog-style query that extends the base GraphQuery
    /// Contains patterns, recursive patterns, and projection specifications
    /// </summary>
    public sealed record DatalogQuery(
        ImmutableList<Pattern> Patterns,
        ImmutableList<RecursivePattern> RecursivePatterns,
        ProjectionSpec? Projection,
        int? MaxRecursionDepth,
        bool ConsistentRead,
        string? Tag) : GraphQuery;
}
