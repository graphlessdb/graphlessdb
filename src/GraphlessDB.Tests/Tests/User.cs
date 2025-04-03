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
    public sealed record User(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        string Username) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
    {
        public static User New(string username)
        {
            var now = DateTime.UtcNow;
            return new User(
                GlobalId.Get<User>(Guid.NewGuid().ToString()),
                VersionDetail.New, now, now, DateTime.MinValue, username);
        }
    }
}