/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed class IntFilter : IValueFilter
    {
        public int? Eq { get; set; }
        public int? Ge { get; set; }
        public int? Gt { get; set; }
        public int? Le { get; set; }
        public int? Lt { get; set; }
    }
}
