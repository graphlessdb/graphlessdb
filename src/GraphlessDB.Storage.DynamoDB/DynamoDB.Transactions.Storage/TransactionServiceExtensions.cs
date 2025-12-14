/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public static class TransactionStoreExtensions
    {
        // Deletes the transaction, only if it has not been update since the specified duration.  A transaction's 
        // "last updated date" is updated when:
        //  - A request is added to the transaction
        //  - The transaction switches to COMMITTED or ROLLED_BACK
        //  - The transaction is marked as completed.  
        // 
        // @param deleteIfAfterMillis the duration to ensure has passed before attempting to delete the record
        // @return true if the transaction was deleted, false if it was not old enough to delete yet.
        // @throws TransactionException if the transaction is not yet completed.
        public static async Task<bool> TryRemoveAsync(this ITransactionStore source, TransactionId id, CancellationToken cancellationToken)
        {
            try
            {
                await source.RemoveAsync(id, cancellationToken);
                return true;
            }
            catch (TransactionNotFoundException)
            {
                return false;
            }
        }

        public static async Task<bool> TryRemoveAsync(this ITransactionStore source, TransactionId id, TimeSpan deleteIfAfterDuration, CancellationToken cancellationToken)
        {
            try
            {
                var transaction = await source.GetAsync(id, true, cancellationToken);
                if (transaction.State is not (TransactionState.Committed or TransactionState.RolledBack))
                {
                    throw new TransactionException(id.Id, "You can only delete a transaction that is committed or rolledback");
                }

                if (transaction.LastUpdateDateTime + deleteIfAfterDuration > DateTime.UtcNow)
                {
                    return false;
                }

                return await source.TryRemoveAsync(id, cancellationToken);
            }
            catch (TransactionNotFoundException)
            {
                // expected, transaction already deleted
                return false;
            }
            catch (ConditionalCheckFailedException ex)
            {
                // Can only happen if the transaction isn't finalized or is already gone.
                throw new TransactionException(id.Id, "Transaction was completed but could not be deleted", ex);
            }
        }
    }
}