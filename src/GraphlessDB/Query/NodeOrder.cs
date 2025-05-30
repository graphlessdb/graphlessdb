/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Query
{
    public sealed record NodeOrder(OrderItem? Item)
    {
        public static readonly NodeOrder Empty = new((OrderItem?)null);
    }
}
