/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Query.Datalog.Core
{
    /// <summary>
    /// Base class for all pattern types in a datalog query
    /// </summary>
    public abstract record Pattern(string Id);

    /// <summary>
    /// Pattern for matching nodes by type and property constraints
    /// Example: ?user hasType User, ?user.Username = "john"
    /// </summary>
    public sealed record NodePattern(
        string Id,
        Variable<INode> Variable,
        string NodeTypeName,
        ImmutableList<PropertyConstraint> PropertyConstraints) : Pattern(Id);

    /// <summary>
    /// Pattern for matching edges between two node variables
    /// Example: ?user1 -[:UserLikesUser]-> ?user2
    /// </summary>
    public sealed record EdgePattern(
        string Id,
        Variable<INode> FromVariable,
        string EdgeTypeName,
        Variable<INode> ToVariable,
        ImmutableList<PropertyConstraint> EdgePropertyConstraints) : Pattern(Id);

    /// <summary>
    /// Pattern for joining two variables together
    /// Used for explicit joins or constraints
    /// </summary>
    public sealed record JoinPattern(
        string Id,
        Variable<IEntity> LeftVariable,
        Variable<IEntity> RightVariable,
        JoinType JoinType) : Pattern(Id);

    /// <summary>
    /// Represents a property constraint for filtering
    /// </summary>
    public sealed record PropertyConstraint(
        string PropertyName,
        IValueFilter Filter);

    /// <summary>
    /// Type of join operation
    /// </summary>
    public enum JoinType
    {
        /// <summary>
        /// Inner join - only rows that match in both sides
        /// </summary>
        Inner,

        /// <summary>
        /// Left outer join - all rows from left, matching rows from right
        /// </summary>
        LeftOuter
    }
}
