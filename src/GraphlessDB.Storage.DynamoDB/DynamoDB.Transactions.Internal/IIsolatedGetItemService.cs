/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public interface IIsolatedGetItemService<T> : IIsolatedGetItemService
        where T : IsolationLevelServiceType
    {
    }

    public interface IIsolatedGetItemService
    {
        Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken);

        Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken);

        Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken);
    }
}