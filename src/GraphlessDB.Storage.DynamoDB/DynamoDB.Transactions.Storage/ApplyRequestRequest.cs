/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using Amazon.DynamoDBv2;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record ApplyRequestRequest(
        Transaction Transaction,
        AmazonDynamoDBRequest Request,
        int RequestId,
        ImmutableDictionary<ItemKey, ItemTransactionState> ItemTransactionStatesByKey,
        ImmutableDictionary<ItemKey, ItemRecord> ItemsToBackupByKey);
}
