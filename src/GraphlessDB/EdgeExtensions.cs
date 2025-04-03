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
    public static class EdgeExtensions
    {
        public static T Update<T>(this T source)
        where T : IEdge
        {
            return source with
            {
                UpdatedAt = DateTime.UtcNow,
            };
        }

        public static T Delete<T>(this T source)
        where T : IEdge
        {
            return source with
            {
                DeletedAt = DateTime.UtcNow,
            };
        }

        public static EdgeKey ToEdgeKey<T>(this T source)
        where T : IEdge
        {
            return new EdgeKey(source.GetType().Name, source.InId, source.OutId);
        }
    }
}