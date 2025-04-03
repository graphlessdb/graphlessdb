/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Linq;

namespace GraphlessDB
{
    public sealed class EnumFilter<T> : IHasValueFilter where T : struct
    {
        public T? Eq { get; set; }
        public T[]? In { get; set; }

        public IValueFilter GetValueFilter()
        {
            return new EnumFilter
            {
                Eq = Eq,
                In = In?.Cast<object>().ToArray()
            };
        }
    }

    public sealed class EnumFilter : IValueFilter
    {
        public object? Eq { get; set; }
        public object[]? In { get; set; }
    }
}
