/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions
{
    public class ItemNotLockedException(string transactionId, string? lockOwningTransactionId, string tableName, ImmutableDictionary<string, AttributeValue> item, Exception? innerException = null) : Exception("Item is not locked by our transaction, is locked by " + lockOwningTransactionId + " for table " + tableName + ", item: " + item, innerException)
    {
        public string TransactionId { get; } = transactionId;

        public string? LockOwningTransactionId { get; } = lockOwningTransactionId;

        public string TableName { get; } = tableName;

        public ImmutableDictionary<string, AttributeValue> Item { get; } = item;
    }
}
