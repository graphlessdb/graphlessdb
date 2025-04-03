/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public static class RequestServiceExtensions
    {
        public static async Task<ImmutableList<ItemKey>> GetItemKeysAsync(
            this IRequestService source, AmazonDynamoDBRequest request, CancellationToken cancellationToken)
        {
            var itemRequestActions = await source.GetItemRequestDetailsAsync(request, cancellationToken);
            return itemRequestActions.Select(v => v.Key).ToImmutableList();
        }
    }
}
