/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record NodeConnectionOptions(INodeFilter? Filter, INodeOrder? Order, int PageSize, string? Tag)
    {
        public static readonly NodeConnectionOptions Default = new(null, null, 25, null);
    }
}
