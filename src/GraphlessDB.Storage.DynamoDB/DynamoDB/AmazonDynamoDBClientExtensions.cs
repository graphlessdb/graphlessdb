/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB
{
    public static class AmazonDynamoDBClientExtensions
    {
        public static async Task BatchWriteItemAsync(
            this IAmazonDynamoDB source,
            BatchWriteItemRequest request,
            BatchWriteItemOptions options,
            CancellationToken cancellationToken)
        {
            var batchedRequests = request.ToBatchedRequests(options.BatchSize);
            foreach (var batchedRequest in batchedRequests)
            {
                await source.BatchWriteItemWithRetryAsync(batchedRequest, options, cancellationToken);
            }
        }

        private static async Task BatchWriteItemWithRetryAsync(
            this IAmazonDynamoDB source,
            BatchWriteItemRequest request,
            BatchWriteItemOptions options,
            CancellationToken cancellationToken)
        {
            var remainingRequest = request;
            var remainingResponse = await source.BatchWriteItemAsync(remainingRequest, cancellationToken);
            var backoff = options.InitialBackoff;
            var attempt = 0;
            while (remainingResponse.UnprocessedItems.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (options.MaxAttempts.HasValue && attempt > options.MaxAttempts.Value)
                {
                    throw new InvalidOperationException("Maximum attempts exceeded");
                }

                // _logger.LogWarning("UnprocessedItems present, will retry. RetryIntervalSeconds = {RetryIntervalSeconds}", backoff.TotalSeconds);
                await Task.Delay(backoff, cancellationToken);

                // Update backoff amount
                backoff *= options.BackoffMultiplier;
                if (backoff > options.MaxBackoff)
                {
                    backoff = options.MaxBackoff;
                }

                remainingRequest = new BatchWriteItemRequest
                {
                    RequestItems = remainingResponse.UnprocessedItems,
                    ReturnConsumedCapacity = remainingRequest.ReturnConsumedCapacity,
                    ReturnItemCollectionMetrics = remainingRequest.ReturnItemCollectionMetrics,
                };

                remainingResponse = await source.BatchWriteItemAsync(remainingRequest, cancellationToken);
            }
        }
    }
}