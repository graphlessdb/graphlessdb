/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Tests
{
    [GraphlessDBSchema("""
    node Car {
        CreatedAt: DateTime
    }
    node User {
        CreatedAt: DateTime
        Username: String
    }
    edge UserLikesUser {
        in LikedByUsers: User
        out LikesUsers: User
        CreatedAt: DateTime
        LikedByUsername: String
        LikesUsername: String
    }
    edge UserOwnsCar {
        in OwnedByUser: User
        out OwnsCars: Car
        CreatedAt: DateTime
    }
    """)]
    public sealed partial class TestGraph : IGraph
    {
    }
}
