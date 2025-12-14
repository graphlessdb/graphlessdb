/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed class StringFilter : IValueFilter
    {
        public string? Eq { get; set; }
        public string? Ne { get; set; }
        public string? Le { get; set; }
        public string? Lt { get; set; }
        public string? Ge { get; set; }
        public string? Gt { get; set; }
        public string? Contains { get; set; }
        public string? NotContains { get; set; }
        public StringRange? Between { get; set; }
        public string? BeginsWith { get; set; }
        public string[]? BeginsWithAny { get; set; }
        public string[]? In { get; set; }
    }
}
