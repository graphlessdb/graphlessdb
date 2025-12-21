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
    public sealed record WorksAtEdge(DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt, string InId, string OutId)
    : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
    {
        public static WorksAtEdge New(Person person, Company company)
        {
            var now = DateTime.UtcNow;
            return new WorksAtEdge(now, now, DateTime.MinValue, person.Id, company.Id);
        }
    }
}
