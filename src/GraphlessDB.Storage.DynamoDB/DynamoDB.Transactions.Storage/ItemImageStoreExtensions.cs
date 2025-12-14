/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public static class ItemImageStoreExtensions
    {
        public static async Task DeleteItemImagesAsync(
            this IItemImageStore source, TransactionId id, CancellationToken cancellationToken)
        {
            var versions = await source.GetTransactionVersions(id, cancellationToken);
            await Task.WhenAll(versions.Select(v => source.DeleteItemImagesAsync(v, cancellationToken)));
        }

        public static async Task DeleteItemImagesAsync(
            this IItemImageStore source, Transaction transaction, CancellationToken cancellationToken)
        {
            await Task.WhenAll(transaction
                .Requests
                .Select(r => new TransactionVersion(transaction.Id, r.Id))
                .Select(v => source.DeleteItemImagesAsync(v, cancellationToken)));
        }
    }
}
