/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB
{
    public sealed class AmazonDynamoDBKeyService(ITableSchemaService tableSchemaService) : IAmazonDynamoDBKeyService
    {
        public async Task<ImmutableDictionary<string, AttributeValue>> CreateKeyMapAsync(
            string tableName,
            ImmutableDictionary<string, AttributeValue> item,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(item);

            var schema = await tableSchemaService.GetTableSchemaAsync(tableName, cancellationToken);
            var key = ImmutableDictionary.CreateBuilder<string, AttributeValue>();
            foreach (var element in schema)
            {
                key.Add(element.AttributeName, item[element.AttributeName]);
            }

            return key.ToImmutableDictionary();
        }
    }
}
