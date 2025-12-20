/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Query.Datalog.Core;

namespace GraphlessDB.Query.Datalog.Execution
{
    /// <summary>
    /// Base class for compiled pattern representations
    /// </summary>
    internal abstract record CompiledPattern(string Id);

    /// <summary>
    /// Compiled node pattern with filter and pushdown information
    /// </summary>
    internal sealed record CompiledNodePattern(
        string Id,
        string VariableName,
        string NodeTypeName,
        INodeFilter? Filter,
        NodePushdownData? PushdownData) : CompiledPattern(Id);

    /// <summary>
    /// Compiled edge pattern with filter information
    /// </summary>
    internal sealed record CompiledEdgePattern(
        string Id,
        string FromVariableName,
        string ToVariableName,
        string EdgeTypeName,
        IEdgeFilter? Filter) : CompiledPattern(Id);

    /// <summary>
    /// Compiled join pattern
    /// </summary>
    internal sealed record CompiledJoinPattern(
        string Id,
        string LeftVariableName,
        string RightVariableName,
        JoinType JoinType) : CompiledPattern(Id);

    /// <summary>
    /// Information about what can be pushed down to the data layer
    /// </summary>
    internal sealed record NodePushdownData(
        string? PropertyName,
        PropertyOperator Operator,
        ImmutableList<string> Values);

    /// <summary>
    /// Property operators supported by the data layer
    /// </summary>
    internal enum PropertyOperator
    {
        Equals,
        StartsWith
    }
}
