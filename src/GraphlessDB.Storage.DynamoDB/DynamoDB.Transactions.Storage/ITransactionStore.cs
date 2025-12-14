/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public interface ITransactionStore
    {
        Task<ImmutableList<Transaction>> ListAsync(
            int limit,
            CancellationToken cancellationToken);

        Dictionary<string, AttributeValue> GetKey(
            TransactionId id);

        Task<bool> ContainsAsync(
            TransactionId id,
            CancellationToken cancellationToken);

        Task AddAsync(
             Transaction transaction,
             CancellationToken cancellationToken);

        Task<Transaction> GetAsync(
            TransactionId id,
            bool forceFetch,
            CancellationToken cancellationToken);

        Task<Transaction> UpdateAsync(
            Transaction transaction,
            CancellationToken cancellationToken);

        Task<Transaction> AppendRequestAsync(
            Transaction transaction,
            AmazonDynamoDBRequest request,
            CancellationToken cancellationToken);

        Task RemoveAsync(
            TransactionId id,
            CancellationToken cancellationToken);
    }
}
