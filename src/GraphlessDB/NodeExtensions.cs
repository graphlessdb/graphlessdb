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
    public static class NodeExtensions
    {
        public static T Update<T>(this T source)
        where T : INode
        {
            return source with
            {
                UpdatedAt = DateTime.UtcNow,
                Version = source.Version with
                {
                    NodeVersion = source.Version.NodeVersion + 1
                }
            };
        }

        public static T Delete<T>(this T source)
        where T : INode
        {
            return source with
            {
                DeletedAt = DateTime.UtcNow,
                Version = source.Version with
                {
                    NodeVersion = source.Version.NodeVersion + 1
                }
            };
        }
    }
}
