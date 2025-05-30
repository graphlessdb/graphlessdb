/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.DynamoDB.Transactions.Storage;

namespace GraphlessDB.DynamoDB.Transactions
{
    public sealed record TransactionConflictItem(
        ItemKey ItemKey,
        ItemRecord ItemRecord,
        TransactionStateValue TransactionStateValue);
}
