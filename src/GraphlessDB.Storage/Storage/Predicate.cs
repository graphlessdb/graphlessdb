/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public static class Predicate
    {
        public static string ParseTypeName(string value)
        {
            var ind1 = value.IndexOf('#');
            var ind2 = value.IndexOf('#', ind1 + 1);
            var ind3 = value.IndexOf('#', ind2 + 1);
            if (ind3 == -1)
            {
                return value[(ind2 + 1)..];
            }

            return value[(ind2 + 1)..ind3];
        }

        public static string ParsePropName(string value)
        {
            var ind1 = value.IndexOf('#');
            var ind2 = value.IndexOf('#', ind1 + 1);
            var ind3 = value.IndexOf('#', ind2 + 1);
            var ind4 = value.IndexOf('#', ind3 + 1);
            if (ind4 == -1)
            {
                return value[(ind3 + 1)..];
            }

            return value[(ind3 + 1)..ind4];
        }
    }
}