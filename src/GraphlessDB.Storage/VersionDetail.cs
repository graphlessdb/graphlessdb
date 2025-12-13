/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record VersionDetail(int NodeVersion, int AllEdgesVersion)
    {
        public static readonly VersionDetail New = new(0, 0);
    }
}