/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public static class PageInfoExtensions
    {
        public static string? GetNullableStartCursor(this PageInfo source)
        {
            return string.IsNullOrEmpty(source.StartCursor)
                ? null
                : source.StartCursor;
        }

        public static string? GetNullableEndCursor(this PageInfo source)
        {
            return string.IsNullOrEmpty(source.EndCursor)
                ? null
                : source.EndCursor;
        }
    }
}
