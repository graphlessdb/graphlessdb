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
    public abstract record INode(string Id, VersionDetail Version, DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt)
        : IEntity(CreatedAt, UpdatedAt, DeletedAt);
}