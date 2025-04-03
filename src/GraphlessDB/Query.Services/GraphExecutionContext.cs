/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Collections;

namespace GraphlessDB.Query.Services
{
    public sealed record GraphExecutionContext(
        IGraphQueryExecutionService GraphQueryService,
        ImmutableTree<string, GraphQueryNode> Query,
        ImmutableDictionary<string, GraphResult> ResultItems);
}
