/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Tests
{
    public sealed record IsAEdge(DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt, string InId, string OutId)
    : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
    {
        public static IsAEdge New(Concept subtype, Concept supertype)
        {
            var now = DateTime.UtcNow;
            return new IsAEdge(now, now, DateTime.MinValue, subtype.Id, supertype.Id);
        }
    }
}
