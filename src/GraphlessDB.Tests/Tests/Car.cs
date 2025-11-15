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
    public sealed record Car(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        string Model) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
    {
        public static Car New(string model)
        {
            var now = DateTime.UtcNow;
            return new Car(
                GlobalId.Get<Car>(Guid.NewGuid().ToString()),
                VersionDetail.New, now, now, DateTime.MinValue, model);
        }
    }
}
