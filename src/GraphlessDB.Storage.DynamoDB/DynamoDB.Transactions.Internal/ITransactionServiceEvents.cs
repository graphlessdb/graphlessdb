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
    public interface ITransactionServiceEvents
    {
        Func<TransactionId, CancellationToken, Task>? OnResumeTransactionFinishAsync { get; set; }

        Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnApplyRequestAsync { get; set; }

        Func<TransactionVersion, CancellationToken, Task>? OnUpdateFullyAppliedRequestsBeginAsync { get; set; }

        Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task>? OnAcquireLockAsync { get; set; }

        Func<TransactionId, bool, CancellationToken, Task>? OnReleaseLocksAsync { get; set; }

        Func<TransactionId, TransactionId, CancellationToken, Task>? OnReleaseLockFromOtherTransactionAsync { get; set; }

        Func<TransactionId, CancellationToken, Task>? OnBackupItemImagesAsync { get; set; }

        Func<TransactionId, CancellationToken, Task<bool>>? OnDoCommitBeginAsync { get; set; }

        Func<TransactionId, CancellationToken, Task>? OnDoRollbackBeginAsync { get; set; }
    }
}