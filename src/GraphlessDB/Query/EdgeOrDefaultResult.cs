/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Query
{
    public sealed record EdgeResult(
        string? ChildCursor,
        string Cursor,
        bool NeedsMoreData,
        bool HasMoreData,
        RelayEdge<IEdge>? Edge) : GraphResult(ChildCursor, Cursor, NeedsMoreData, HasMoreData);
}
