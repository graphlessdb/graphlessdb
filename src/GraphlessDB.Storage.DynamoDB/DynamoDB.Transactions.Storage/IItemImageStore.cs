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
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public interface IItemImageStore
    {
        Dictionary<string, AttributeValue> GetKey(
            TransactionVersion transactionVersion);

        Task<ImmutableList<TransactionVersion>> GetTransactionVersions(
            TransactionId id,
            CancellationToken cancellationToken);

        Task<ImmutableList<ItemRecord>> GetItemImagesAsync(
            TransactionVersion transactionVersion,
            CancellationToken cancellationToken);

        Task AddItemImagesAsync(
            TransactionVersion transactionVersion,
            ImmutableList<ItemRecord> items,
            CancellationToken cancellationToken);

        Task DeleteItemImagesAsync(
            TransactionVersion transactionVersion,
            CancellationToken cancellationToken);
    }
}
