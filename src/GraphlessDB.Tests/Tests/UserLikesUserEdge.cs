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
    public sealed record UserLikesUserEdge(DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt, string InId, string OutId, string LikedByUsername, string LikesUsername)
    : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId)
    {
        public static UserLikesUserEdge New(User inNode, User outNode)
        {
            var now = DateTime.UtcNow;
            return new UserLikesUserEdge(now, now, DateTime.MinValue, inNode.Id, outNode.Id, inNode.Username, outNode.Username);
        }
    }
}