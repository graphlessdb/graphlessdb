/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    public sealed class DynamoDBTransactionTestOptions
    {
        public DynamoDBTransactionTestOptions()
        {
            TransactionTableName = "TransactionTable";
            ItemImageTableName = "ItemImageTable";
        }

        public string TransactionTableName { get; set; }

        public string ItemImageTableName { get; set; }
    }
}
