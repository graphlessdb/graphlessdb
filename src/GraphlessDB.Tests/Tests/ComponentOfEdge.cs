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
    public sealed record ComponentOfEdge(DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt, string InId, string OutId, int Quantity)
    : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
    {
        public static ComponentOfEdge New(Product component, Product parent, int quantity)
        {
            var now = DateTime.UtcNow;
            return new ComponentOfEdge(now, now, DateTime.MinValue, component.Id, parent.Id, quantity);
        }
    }
}
