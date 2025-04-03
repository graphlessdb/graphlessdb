/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public interface IVersionedItemStore
    {
        ItemResponseAndTransactionState<ItemRecord> GetItemRecordAndTransactionState(
            ItemKey itemKey,
            Dictionary<string, AttributeValue> item);

        Tuple<Dictionary<string, AttributeValue>, TransactionStateValue> GetItemRecordAndTransactionState(
            Dictionary<string, AttributeValue> item);

        Task<ImmutableList<ItemRecord>> GetItemsToBackupAsync(
            AmazonDynamoDBRequest request, CancellationToken cancellationToken);

        Task<ImmutableDictionary<ItemKey, ItemTransactionState>> AcquireLocksAsync(
            Transaction transaction,
            AmazonDynamoDBRequest request,
            CancellationToken cancellationToken);

        Task<AmazonWebServiceResponse> ApplyRequestAsync(
            ApplyRequestRequest request,
            CancellationToken cancellationToken);

        Task ReleaseLocksAsync(
            Transaction transaction,
            bool rollback,
            ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey,
            CancellationToken cancellationToken);

        Task ReleaseLocksAsync(
            TransactionId id,
            TransactionId owningTransactionId,
            ImmutableList<ItemKey> itemKeys,
            bool rollback,
            ImmutableDictionary<ItemKey, ItemTransactionState> itemTransactionStatesByKey,
            ImmutableDictionary<ItemKey, ItemRecord> rollbackImagesByKey,
            CancellationToken cancellationToken);
    }
}
