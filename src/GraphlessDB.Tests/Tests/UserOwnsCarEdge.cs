/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Tests
{
    public sealed record UserOwnsCarEdge(DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt, string InId, string OutId)
    : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
    {
        public static UserOwnsCarEdge New(string inId, string outId)
        {
            var now = DateTime.UtcNow;
            return new UserOwnsCarEdge(now, now, DateTime.MinValue, inId, outId);
        }
    }
}
