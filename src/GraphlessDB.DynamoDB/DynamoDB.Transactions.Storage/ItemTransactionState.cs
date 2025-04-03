/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record ItemTransactionState(
        ItemKey Key,
        bool Exists,
        string? TransactionId,
        DateTime? LastUpdatedDate,
        bool IsTransient,
        bool IsApplied,
        LockedItemRequestAction LockItemRequestAction)
    {
        public static ItemTransactionState Create(ItemKey key, TransactionStateValue value, LockedItemRequestAction lockItemRequestAction)
        {
            return new ItemTransactionState(
                key,
                value.Exists,
                value.TransactionId,
                value.LastUpdatedDate,
                value.IsTransient,
                value.IsApplied,
                lockItemRequestAction);
        }
    }
}
