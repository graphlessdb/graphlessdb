/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Storage;

namespace GraphlessDB.Tests
{
    public sealed record PropertyNode(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        string Name) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
    {
        public static PropertyNode New(string name)
        {
            var now = DateTime.UtcNow;
            return new PropertyNode(
                GlobalId.Get<PropertyNode>(Guid.NewGuid().ToString()),
                VersionDetail.New, now, now, DateTime.MinValue, name);
        }
    }
}
