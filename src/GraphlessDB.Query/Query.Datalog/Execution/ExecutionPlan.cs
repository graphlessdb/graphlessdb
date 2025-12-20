/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.Query.Datalog.Execution
{
    /// <summary>
    /// Represents an optimized execution plan for a datalog query.
    /// Contains compiled patterns in the optimal execution order.
    /// </summary>
    internal sealed record ExecutionPlan(
        ImmutableList<CompiledPattern> RegularPatterns);
}
