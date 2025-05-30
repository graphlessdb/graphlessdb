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
    public sealed record TransactionStateValue(
        bool Exists,
        string? TransactionId,
        DateTime? LastUpdatedDate,
        bool IsTransient,
        bool IsApplied);
}
