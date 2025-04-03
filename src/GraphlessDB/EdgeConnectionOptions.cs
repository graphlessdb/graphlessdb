/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record EdgeConnectionOptions(IEdgeFilter? Filter, IEdgeOrder? Order, int PageSize, string? Tag)
    {
        public static readonly EdgeConnectionOptions Default = new(null, null, 25, null);
    }

    public sealed record EdgeConnectionOptions<TFilter, TOrder>(TFilter? Filter, TOrder? Order, int PageSize, string? Tag)
        where TFilter : class, IEdgeFilter
        where TOrder : class, IEdgeOrder
    {
        public static readonly EdgeConnectionOptions<TFilter, TOrder> Default = new(null, null, 25, null);
    }
}
