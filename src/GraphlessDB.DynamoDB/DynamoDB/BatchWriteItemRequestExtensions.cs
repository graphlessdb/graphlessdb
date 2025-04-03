/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.Linq;

namespace GraphlessDB.DynamoDB
{
    public static class BatchWriteItemRequestExtensions
    {
        public static ImmutableList<BatchWriteItemRequest> ToBatchedRequests(this BatchWriteItemRequest source, int batchSize)
        {
            return source
                .RequestItems
                .SelectMany(kv => kv.Value.Select(request => new Tuple<string, WriteRequest>(kv.Key, request)))
                .ToImmutableListBatches(batchSize)
                .Select(batch => new BatchWriteItemRequest
                {
                    RequestItems = batch
                        .GroupBy(k => k.Item1, v => v.Item2)
                        .ToDictionary(k => k.Key, v => v.ToList()),
                    ReturnConsumedCapacity = source.ReturnConsumedCapacity,
                    ReturnItemCollectionMetrics = source.ReturnItemCollectionMetrics,
                })
                .ToImmutableList();
        }
    }
}