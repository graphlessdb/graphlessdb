/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed record ValueFilterItem(string Name, IValueFilter Value)
    {
        public T Get<T>() where T : IValueFilter
        {
            return (T)Value;
        }
    }
}
