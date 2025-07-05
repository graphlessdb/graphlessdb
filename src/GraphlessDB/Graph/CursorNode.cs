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
        public static readonly CursorNode EndOfDataNode = new(null, null, null, null, null, null, null, Guid.Empty.ToString());

        public static CursorNode CreateEndOfData()
        {
            // NOTE If EndOfData is set it signifies that this is the last node.
            // DISCUSSION If its unique then "FromCursorExclusive" when rehydrated doesn't work.
            //            Setting back to static single value to see if there are any issues with that.
            return EndOfDataNode;
        }
    }
}
