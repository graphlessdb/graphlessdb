/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.Graph
{
    // The cursor has 
    // - a primary subject+partition which defines the ultimate position, this can be used to fetch a node again
    // - addition subject+partition info, this can be used as the exclusive start key to carry on querying within each partition 
    public sealed record HasTypeCursor(string Subject, string Partition, ImmutableList<HasTypeCursorQueryCursor> QueryCursors);
}
