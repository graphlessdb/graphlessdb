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
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB
{
    public interface IAmazonDynamoDBKeyService
    {
        Task<ImmutableDictionary<string, AttributeValue>> CreateKeyMapAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> item,
            CancellationToken cancellationToken);
    }
}
