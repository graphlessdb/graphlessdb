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
    public sealed record TransactionId
    {
        public TransactionId(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
            {
                throw new ArgumentException("Cannot be null, empty or whitespace", nameof(id));
            }

            Id = id;
        }

        public string Id { get; init; }
    }
}
