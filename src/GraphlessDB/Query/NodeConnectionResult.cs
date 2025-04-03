/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Query
{
    public sealed record NodeConnectionResult(
        string? ChildCursor,    // Dependant connection position
        string Cursor,          // Prefiltered connection position
        bool NeedsMoreData,     // More data is required from the dependant connection
        bool HasMoreData,       // More data is available to process within the local prefiltered connection
        Connection<RelayEdge<INode>, INode> Connection) : GraphResult(ChildCursor, Cursor, NeedsMoreData, HasMoreData);
}
