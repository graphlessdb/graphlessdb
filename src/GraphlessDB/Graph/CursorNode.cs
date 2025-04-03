/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Graph
{
    // NOTE If EndOfData is set it signifies that this is the last node
    //      It must be a unique string so that no two EndOfData nodes are equivalent
    public sealed record CursorNode(
        HasTypeCursor? HasType,
        HasPropCursor? HasProp,
        HasInEdgeCursor? HasInEdge,
        HasInEdgePropCursor? HasInEdgeProp,
        HasOutEdgeCursor? HasOutEdge,
        HasOutEdgePropCursor? HasOutEdgeProp,
        IndexedCursor? Indexed,
        string? EndOfData)
    {
        public static readonly CursorNode Empty = new(null, null, null, null, null, null, null, null);

        public static CursorNode CreateEndOfData()
        {
            return Empty with
            {
                EndOfData = Guid.NewGuid().ToString()
            };
        }
    }
}
