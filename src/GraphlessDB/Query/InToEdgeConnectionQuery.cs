/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Query
{
    public sealed record InToEdgeConnectionQuery(
        string EdgeTypeName,
        string NodeInTypeName,
        string NodeOutTypeName,
        IEdgeFilter? Filter,
        IEdgeOrder? Order,
        ConnectionArguments Page,
        int PreFilteredPageSize,
        bool ConsistentRead,
        string? Tag)
        : ToEdgeConnectionQuery(
            EdgeTypeName,
            Filter,
            Order,
            Page,
            PreFilteredPageSize,
            ConsistentRead,
            Tag);
}
