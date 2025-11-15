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
    public sealed record Manufacturer(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        string Name) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
    {
        public static Manufacturer New(string name)
        {
            var now = DateTime.UtcNow;
            return new Manufacturer(
                GlobalId.Get<Manufacturer>(Guid.NewGuid().ToString()),
                VersionDetail.New, now, now, DateTime.MinValue, name);
        }
    }
}
