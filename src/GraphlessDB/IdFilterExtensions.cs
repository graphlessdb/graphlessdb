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
    public static class IdFilterExtensions
    {
        public static bool IsMatch(this IdFilter source, string value)
        {
            if (source.Eq != null && value != source.Eq)
            {
                return false;
            }

            if (source.In != null && !source.In.Where(v => v == value).Any())
            {
                return false;
            }

            if (source.Ne != null && value == source.Ne)
            {
                return false;
            }

            return true;
        }
    }
}

