/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using Amazon.Runtime;
using GraphlessDB.DynamoDB.Transactions.Storage;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public sealed record ApplyRequestResponse(
        Transaction Transaction,
        AmazonWebServiceResponse AmazonWebServiceResponse,
        ImmutableDictionary<ItemKey, ItemTransactionState> ItemTransactionStates,
        ImmutableDictionary<ItemKey, ItemRecord> ItemsToBackupByKey);
}
