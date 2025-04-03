/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public interface IRequestService
    {
        Task<ImmutableList<LockedItemRequestAction>> GetItemRequestActionsAsync(
           Transaction transaction, CancellationToken cancellationToken);

        Task<ImmutableList<ItemRequestDetail>> GetItemRequestDetailsAsync(
            AmazonDynamoDBRequest request, CancellationToken cancellationToken);
    }
}