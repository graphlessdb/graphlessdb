/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading.Tasks;

namespace GraphlessDB.Query
{
    public record WhereNodeConnectionQuery(
        Func<WhereRelayNodeContext<IGraph, INode>, Task<bool>> Predicate,
        ConnectionArguments Page,
        int PreFilteredPageSize,
        bool ConsistentRead,
        string? Tag) : GraphQuery;
}
