/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;

namespace GraphlessDB.DynamoDB.Transactions
{
    public class TransactionConflictedException(string id, ImmutableList<TransactionConflictItem> conflictingItems, string? message = null, Exception? innerException = null) : Exception(message, innerException)
    {
        public string Id { get; } = id;

        public ImmutableList<TransactionConflictItem> ConflictingItems { get; } = conflictingItems;
    }
}
