using GraphlessDB.Domain.Graph;


/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */
namespace GraphlessDB.Query
{
    public record NodeConnectionQuery(
        string Type,
        INodeFilter? Filter,
        INodeOrder? Order,
        ConnectionArguments Page,
        int PreFilteredPageSize,
        bool ConsistentRead,
        string? Tag) : GraphQuery;
}
