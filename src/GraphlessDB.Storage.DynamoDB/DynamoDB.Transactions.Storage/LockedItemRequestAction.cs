/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record LockedItemRequestAction(ItemKey Key, int RequestId, RequestAction RequestAction);
}
