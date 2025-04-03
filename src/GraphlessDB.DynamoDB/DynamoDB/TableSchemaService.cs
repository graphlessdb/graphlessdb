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
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB
{
    public sealed class TableSchemaService(IAmazonDynamoDB client) : ITableSchemaService, IDisposable
    {
        private readonly IAmazonDynamoDB _client = client;
        private readonly SemaphoreSlim _lock = new(1);
        private ImmutableDictionary<string, ImmutableList<KeySchemaElement>> _tableSchemaCache = ImmutableDictionary<string, ImmutableList<KeySchemaElement>>.Empty;
        private bool _disposedValue;

        public async Task<ImmutableList<KeySchemaElement>> GetTableSchemaAsync(string tableName, CancellationToken cancellationToken)
        {
            if (_tableSchemaCache.TryGetValue(tableName, out var schema))
            {
                return schema;
            }

            await _lock.WaitAsync(cancellationToken);
            try
            {
                if (_tableSchemaCache.TryGetValue(tableName, out var schema2))
                {
                    return schema2;
                }

                var fetchedSchema = await _client.DescribeTableAsync(new DescribeTableRequest(tableName), cancellationToken);
                _tableSchemaCache = _tableSchemaCache.Add(fetchedSchema.Table.TableName, [.. fetchedSchema.Table.KeySchema]);
                return _tableSchemaCache[tableName];
            }
            finally
            {
                _lock.Release();
            }
        }

        private void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // NOTE: This is being disposed before the above function finishes
                    // _lock.Dispose();
                }

                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
