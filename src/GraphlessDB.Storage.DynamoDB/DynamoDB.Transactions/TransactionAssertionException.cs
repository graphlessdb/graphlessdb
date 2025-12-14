/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.DynamoDB.Transactions
{
    public class TransactionAssertionException(string id, string? message = null, Exception? innerException = null) : Exception(message, innerException)
    {
        public string Id { get; } = id;

        public static void TxAssert(bool assertion, string transactionId, string message)
        {
            if (!assertion)
            {
                throw new TransactionAssertionException(transactionId, message);
            }
        }
    }
}
