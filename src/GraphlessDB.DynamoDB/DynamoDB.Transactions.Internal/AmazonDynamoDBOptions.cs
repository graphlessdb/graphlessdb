/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public sealed class AmazonDynamoDBOptions
    {
        // https://aws.amazon.com/about-aws/whats-new/2022/09/amazon-dynamodb-supports-100-actions-per-transaction/
        public static readonly int DefaultTransactGetItemCountMaxValue = 100;
        public static readonly int DefaultTransactWriteItemCountMaxValue = 100;
        public static readonly int DefaultBatchGetItemCountMaxValue = 100;
        public static readonly int DefaultBatchWriteItemCountMaxValue = 25;
        public static readonly bool DefaultQuickTransactionsEnabled = true;
        public static readonly TimeSpan DefaultTransactionStaleDuration = TimeSpan.FromSeconds(10);

        public AmazonDynamoDBOptions()
        {
            QuickTransactionsEnabled = DefaultQuickTransactionsEnabled;
            TransactGetItemCountMaxValue = DefaultTransactGetItemCountMaxValue;
            TransactWriteItemCountMaxValue = DefaultTransactWriteItemCountMaxValue;
            BatchGetItemCountMaxValue = DefaultBatchGetItemCountMaxValue;
            BatchWriteItemCountMaxValue = DefaultBatchWriteItemCountMaxValue;
            TransactionStaleDuration = DefaultTransactionStaleDuration;
        }

        public bool QuickTransactionsEnabled { get; set; }

        public int TransactGetItemCountMaxValue { get; set; }

        public int TransactWriteItemCountMaxValue { get; set; }

        public int BatchGetItemCountMaxValue { get; set; }

        public int BatchWriteItemCountMaxValue { get; set; }

        public TimeSpan TransactionStaleDuration { get; set; }
    }
}
