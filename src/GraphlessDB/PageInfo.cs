/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record PageInfo(bool HasNextPage, bool HasPreviousPage, string StartCursor, string EndCursor)
    {
        public static readonly PageInfo Empty = new(false, false, string.Empty, string.Empty);
    }
}