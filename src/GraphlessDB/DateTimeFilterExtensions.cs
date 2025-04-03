/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB
{
    public static class DateTimeFilterExtensions
    {
        public static bool IsMatch(this DateTimeFilter source, DateTime value)
        {
            if (source.Eq != null && value != source.Eq)
            {
                return false;
            }

            // if (filter.Ne != null && value == filter.Ne)
            // {
            //     return false;
            // }

            if (source.Le != null && !(value <= source.Le))
            {
                return false;
            }

            // public string? Lt { get; set; }
            if (source.Lt != null && !(value < source.Lt))
            {
                return false;
            }

            // public string? Ge { get; set; }
            if (source.Ge != null && !(value >= source.Ge))
            {
                return false;
            }

            // public string? Gt { get; set; }
            if (source.Gt != null && !(value > source.Gt))
            {
                return false;
            }

            return true;
        }
    }
}
