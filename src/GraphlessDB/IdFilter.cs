/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed class IdFilter : IValueFilter
    {
        public string? Eq { get; set; }
        public string? Ne { get; set; }
        public string[]? In { get; set; }
    }
}
