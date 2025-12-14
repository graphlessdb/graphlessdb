/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record EdgeOptions(string? Tag)
    {
        public static readonly EdgeOptions Default = new((string?)null);
    }
}
