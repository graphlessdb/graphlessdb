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
    public static class EntityExtensions
    {
        public static string ToKey(this IEntity source)
        {
            return source switch
            {
                IEdge edge => edge.ToEdgeKey().ToString(),
                INode node => node.Id,
                _ => throw new NotSupportedException(),
            };
        }
    }
}