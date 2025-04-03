/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.DynamoDB
{
    public sealed class BatchWriteItemOptions(int batchSize)
    {
        public static readonly BatchWriteItemOptions Default = new(25);

        public int BatchSize { get; } = batchSize;

        public int? MaxAttempts { get; }

        public TimeSpan InitialBackoff { get; }

        public TimeSpan MaxBackoff { get; }

        public double BackoffMultiplier { get; }
    }
}