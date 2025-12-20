/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Query.Datalog.Core
{
    /// <summary>
    /// Specifies which variables to return from a datalog query and how to order/paginate them
    /// </summary>
    public sealed record ProjectionSpec(
        ImmutableList<VariableProjection> Variables,
        ConnectionArguments Page,
        int PreFilteredPageSize);

    /// <summary>
    /// Represents a single variable being projected in the result set
    /// </summary>
    public sealed record VariableProjection(
        string VariableName,
        Type EntityType,
        INodeOrder? Order);
}
