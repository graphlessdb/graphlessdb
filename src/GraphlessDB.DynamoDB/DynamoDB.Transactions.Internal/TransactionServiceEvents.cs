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

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public sealed class TransactionServiceEvents : ITransactionServiceEvents
    {
        public Func<TransactionId, CancellationToken, Task>? OnResumeTransactionFinishAsync { get; set; }

        public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnApplyRequestAsync { get; set; }

        public Func<TransactionVersion, CancellationToken, Task>? OnUpdateFullyAppliedRequestsBeginAsync { get; set; }

        public Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnAcquireLockAsync { get; set; }

        public Func<TransactionId, CancellationToken, Task>? OnBackupItemImagesAsync { get; set; }

        public Func<TransactionId, CancellationToken, Task<bool>>? OnDoCommitBeginAsync { get; set; }

        public Func<TransactionId, CancellationToken, Task>? OnDoRollbackBeginAsync { get; set; }

        public Func<TransactionId, bool, CancellationToken, Task>? OnReleaseLocksAsync { get; set; }

        public Func<TransactionId, TransactionId, CancellationToken, Task>? OnReleaseLockFromOtherTransactionAsync { get; set; }
    }
}