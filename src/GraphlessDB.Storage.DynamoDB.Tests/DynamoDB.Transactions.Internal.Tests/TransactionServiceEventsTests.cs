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
using GraphlessDB.DynamoDB.Transactions;
using GraphlessDB.DynamoDB.Transactions.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Internal.Tests
{
    [TestClass]
    public sealed class TransactionServiceEventsTests
    {
        [TestMethod]
        public void OnResumeTransactionFinishAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnResumeTransactionFinishAsync);
        }

        [TestMethod]
        public void OnResumeTransactionFinishAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, CancellationToken, Task> handler = (id, ct) => Task.CompletedTask;
            
            events.OnResumeTransactionFinishAsync = handler;
            
            Assert.AreEqual(handler, events.OnResumeTransactionFinishAsync);
        }

        [TestMethod]
        public void OnApplyRequestAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnApplyRequestAsync);
        }

        [TestMethod]
        public void OnApplyRequestAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task> handler = (id, req, ct) => Task.CompletedTask;
            
            events.OnApplyRequestAsync = handler;
            
            Assert.AreEqual(handler, events.OnApplyRequestAsync);
        }

        [TestMethod]
        public void OnUpdateFullyAppliedRequestsBeginAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnUpdateFullyAppliedRequestsBeginAsync);
        }

        [TestMethod]
        public void OnUpdateFullyAppliedRequestsBeginAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionVersion, CancellationToken, Task> handler = (ver, ct) => Task.CompletedTask;
            
            events.OnUpdateFullyAppliedRequestsBeginAsync = handler;
            
            Assert.AreEqual(handler, events.OnUpdateFullyAppliedRequestsBeginAsync);
        }

        [TestMethod]
        public void OnAcquireLockAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnAcquireLockAsync);
        }

        [TestMethod]
        public void OnAcquireLockAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, AmazonDynamoDBRequest, CancellationToken, Task> handler = (id, req, ct) => Task.CompletedTask;
            
            events.OnAcquireLockAsync = handler;
            
            Assert.AreEqual(handler, events.OnAcquireLockAsync);
        }

        [TestMethod]
        public void OnBackupItemImagesAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnBackupItemImagesAsync);
        }

        [TestMethod]
        public void OnBackupItemImagesAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, CancellationToken, Task> handler = (id, ct) => Task.CompletedTask;
            
            events.OnBackupItemImagesAsync = handler;
            
            Assert.AreEqual(handler, events.OnBackupItemImagesAsync);
        }

        [TestMethod]
        public void OnDoCommitBeginAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnDoCommitBeginAsync);
        }

        [TestMethod]
        public void OnDoCommitBeginAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, CancellationToken, Task<bool>> handler = (id, ct) => Task.FromResult(true);
            
            events.OnDoCommitBeginAsync = handler;
            
            Assert.AreEqual(handler, events.OnDoCommitBeginAsync);
        }

        [TestMethod]
        public void OnDoRollbackBeginAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnDoRollbackBeginAsync);
        }

        [TestMethod]
        public void OnDoRollbackBeginAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, CancellationToken, Task> handler = (id, ct) => Task.CompletedTask;
            
            events.OnDoRollbackBeginAsync = handler;
            
            Assert.AreEqual(handler, events.OnDoRollbackBeginAsync);
        }

        [TestMethod]
        public void OnReleaseLocksAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnReleaseLocksAsync);
        }

        [TestMethod]
        public void OnReleaseLocksAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, bool, CancellationToken, Task> handler = (id, b, ct) => Task.CompletedTask;
            
            events.OnReleaseLocksAsync = handler;
            
            Assert.AreEqual(handler, events.OnReleaseLocksAsync);
        }

        [TestMethod]
        public void OnReleaseLockFromOtherTransactionAsyncInitiallyNull()
        {
            var events = new TransactionServiceEvents();
            
            Assert.IsNull(events.OnReleaseLockFromOtherTransactionAsync);
        }

        [TestMethod]
        public void OnReleaseLockFromOtherTransactionAsyncCanBeSet()
        {
            var events = new TransactionServiceEvents();
            Func<TransactionId, TransactionId, CancellationToken, Task> handler = (id1, id2, ct) => Task.CompletedTask;
            
            events.OnReleaseLockFromOtherTransactionAsync = handler;
            
            Assert.AreEqual(handler, events.OnReleaseLockFromOtherTransactionAsync);
        }
    }
}
