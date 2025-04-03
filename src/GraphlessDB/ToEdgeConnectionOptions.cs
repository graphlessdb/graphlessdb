/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record ToEdgeConnectionOptions(IEdgeFilter? Filter, IEdgeOrder? Order, int PageSize, string? Tag)
    {
        public static readonly ToEdgeConnectionOptions Default = new(null, null, 25, null);
    }
}
