/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record NodeConnectionOptions<TNodeFilter, TNodeOrder>(TNodeFilter? Filter, TNodeOrder? Order, string? Tag)
        where TNodeFilter : class, INodeFilter
        where TNodeOrder : class, INodeOrder
    {
        public static readonly NodeConnectionOptions<TNodeFilter, TNodeOrder> Empty = new(null, null, null);
    }
}
