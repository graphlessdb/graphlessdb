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
    public static class EnumFilterExtensions
    {
        public static bool IsMatch<T>(this EnumFilter<T> source, T? value) where T : struct
        {
            return ((EnumFilter)source.GetValueFilter()).IsMatch(value);
        }

        public static bool IsMatch(this EnumFilter source, object? value)
        {
            if (source.Eq != null && !source.Eq.Equals(value))
            {
                return false;
            }

            if (source.In != null && !source.In.Contains(value))
            {
                return false;
            }

            return true;
        }

    }
}
