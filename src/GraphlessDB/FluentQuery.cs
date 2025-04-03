/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Collections;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;

namespace GraphlessDB
{
    public interface IFluentQuery
    {
        IGraphQueryExecutionService GraphQueryService { get; }

        ImmutableTree<string, GraphQueryNode> Query { get; }

        string Key { get; }
    };
}
