/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record LockedRequestData(
        ImmutableList<ItemKey> ItemKeys,
        ImmutableDictionary<ItemKey, ItemRecord> ItemsByKey,
        ImmutableDictionary<ItemKey, ItemTransactionState> ItemTransactionStatesByKey,
        ImmutableDictionary<ItemKey, LockedItemRequestAction> ItemRequestActionsByKey);
}
