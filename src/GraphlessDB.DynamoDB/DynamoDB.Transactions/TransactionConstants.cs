/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.DynamoDB.Transactions
{
    public static class TransactionConstants
    {
        public const int ItemLockAcquireAttempts = 3;
        public const int ItemCommitAttempts = 2;
        public const int TxLockAcquireAttempts = 2;
        public const int TxLockContentionResolutionAttempts = 3;
        public const string BooleanTrueAttrVal = "1";
    }
}
