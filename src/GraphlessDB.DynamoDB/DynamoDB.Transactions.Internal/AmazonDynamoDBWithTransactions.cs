/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB.DynamoDB.Transactions.Storage;
using GraphlessDB.Linq;
using GraphlessDB.Threading;
using Microsoft.Extensions.Options;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public sealed class AmazonDynamoDBWithTransactions(
        IOptionsSnapshot<AmazonDynamoDBOptions> options,
        IAmazonDynamoDB amazonDynamoDB,
        IIsolatedGetItemService<UnCommittedIsolationLevelServiceType> unCommittedIsolatedGetItemService,
        IIsolatedGetItemService<CommittedIsolationLevelServiceType> committedIsolatedGetItemService,
        ITransactionStore transactionStore,
        IVersionedItemStore versionedItemStore,
        IItemImageStore itemImageStore,
        IRequestService requestService,
        ITransactionServiceEvents transactionServiceEvents,
        IFullyAppliedRequestService fullyAppliedRequestService) : IAmazonDynamoDBWithTransactions
    {
#pragma warning disable CS0649
        private static readonly bool s_reRouteRequests;
#pragma warning restore CS0649  

        public IDynamoDBv2PaginatorFactory Paginators => amazonDynamoDB.Paginators;

        public IClientConfig Config => amazonDynamoDB.Config;

        public async Task<TransactionId> BeginTransactionAsync(CancellationToken cancellationToken)
        {
            var transaction = Transaction.CreateNew();
            await transactionStore.AddAsync(transaction, cancellationToken);
            return transaction.GetId();
        }

        public async Task<TransactionId> ResumeTransactionAsync(TransactionId id, CancellationToken cancellationToken)
        {
            var transaction = await transactionStore.GetAsync(id, true, cancellationToken);
            var onResumeTransactionFinishAsync = transactionServiceEvents.OnResumeTransactionFinishAsync;
            if (onResumeTransactionFinishAsync != null)
            {
                await onResumeTransactionFinishAsync(id, cancellationToken);
            }

            return id;
        }

        public async Task CommitTransactionAsync(
            TransactionId id,
            CancellationToken cancellationToken)
        {
            await CompleteTransactionAsync(id, false, cancellationToken);
        }

        public async Task RollbackTransactionAsync(TransactionId id, CancellationToken cancellationToken)
        {
            await CompleteTransactionAsync(id, true, cancellationToken);
        }

        private async Task CompleteTransactionAsync(
            TransactionId id,
            bool rollback,
            CancellationToken cancellationToken)
        {
            await Retry.RunAsync(async () =>
            {
                var transaction = await transactionStore.GetAsync(id, false, cancellationToken);
                try
                {
                    var processOutstandingRequests = !rollback;
                    transaction = await ProcessTransactionAsync(transaction, processOutstandingRequests, cancellationToken);
                    switch (transaction.State)
                    {
                        case TransactionState.Active:
                            transaction = await transactionStore.UpdateAsync(transaction with
                            {
                                Version = transaction.Version + 1,
                                State = rollback ? TransactionState.RollingBack : TransactionState.Committing
                            }, cancellationToken);
                            await ProcessTransactionAsync(transaction, processOutstandingRequests, cancellationToken);
                            return;
                        case TransactionState.Committed:
                            if (rollback)
                            {
                                throw new TransactionCommittedException(transaction.Id, "The transaction is already committed");
                            }
                            return;
                        case TransactionState.RolledBack:
                            if (rollback)
                            {
                                return;
                            }
                            throw new TransactionRolledBackException(id.Id, "The transaction is already rolled back");
                        default:
                            throw new TransactionException(transaction.Id, "Unexpected state " + transaction.State);
                    }
                }
                catch (Exception ex) when (ex is TransactionCommittedException && !rollback)
                {
                    return;
                }
                catch (Exception ex) when (ex is TransactionRolledBackException && rollback)
                {
                    return;
                }
                catch (TransactionException)
                {
                    transaction = await transactionStore.GetAsync(transaction.GetId(), true, cancellationToken);
                    throw;
                }
            }, new RetryOptions
            {
                Condition = v => new RetryConditionResponse(
                    v.Exception is TransactionException && v.RetryAttempt < 3, TimeSpan.Zero)
            }, cancellationToken);
        }

        private IIsolatedGetItemService GetIsolatedGetItemService(IsolationLevel isolationLevel)
        {
            return isolationLevel switch
            {
                IsolationLevel.UnCommitted => unCommittedIsolatedGetItemService,
                IsolationLevel.Committed => committedIsolatedGetItemService,
                _ => throw new ArgumentException("Unrecognized isolation level: " + isolationLevel),
            };
        }

        // private void BreakLock(string tableName, ImmutableDictionary<string, AttributeValue> item, string transactionId)
        // {
        //     // Breaks an item lock and leaves the item intact, leaving an item in an unknown state.  Only works if the owning transaction
        //     // does not exist. 
        //     // 
        //     //   1) It could leave an item that should not exist (was inserted only for obtaining the lock)
        //     //   2) It could replace the item with an old copy of the item from an unknown previous transaction
        //     //   3) A request from an earlier transaction could be applied a second time
        //     //   4) Other conditions of this nature 
        //     _logger.LogWarning("Breaking a lock on table " + tableName + " for transaction " + transactionId + " for item " + item + ".  This will leave the item in an unknown state");
        //     UnlockItemUnsafe(tableName, item, transactionId);
        // }

        public async Task<RunHouseKeepingResponse> RunHouseKeepingAsync(RunHouseKeepingRequest request, CancellationToken cancellationToken)
        {
            var transactions = await transactionStore.ListAsync(request.Limit, cancellationToken);
            var transactionResponses = await Task.WhenAll(transactions.Select(transaction => TryRunHouseKeepingAsync(request, transaction, cancellationToken)));
            return new RunHouseKeepingResponse([.. transactionResponses]);
        }

        private async Task<HouseKeepTransactionResponse> TryRunHouseKeepingAsync(RunHouseKeepingRequest request, Transaction transaction, CancellationToken cancellationToken)
        {
            try
            {
                switch (transaction.State)
                {
                    case TransactionState.Committed:
                    case TransactionState.RolledBack:
                        await transactionStore.TryRemoveAsync(transaction.GetId(), request.DeleteAfterDuration, cancellationToken);
                        return new HouseKeepTransactionResponse(transaction, HouseKeepTransactionAction.Removed, null);
                    case TransactionState.Active:
                        if ((transaction.LastUpdateDateTime + request.RollbackAfterDuration) < DateTime.UtcNow)
                        {
                            try
                            {
                                await RollbackTransactionAsync(transaction.GetId(), cancellationToken);
                            }
                            catch (TransactionCompletedException)
                            {
                                // Transaction is already completed, ignore
                            }
                            return new HouseKeepTransactionResponse(transaction, HouseKeepTransactionAction.RolledBack, null);
                        }
                        return new HouseKeepTransactionResponse(transaction, HouseKeepTransactionAction.None, null);
                    case TransactionState.Committing: // NOTE: falling through to ROLLED_BACK
                    case TransactionState.RollingBack:
                        // This could call either commit or rollback - they'll both do the right thing if it's already committed
                        try
                        {
                            await RollbackTransactionAsync(transaction.GetId(), cancellationToken);
                        }
                        catch (TransactionCompletedException)
                        {
                            // Transaction is already completed, ignore
                        }
                        return new HouseKeepTransactionResponse(transaction, HouseKeepTransactionAction.RolledBack, null);
                    default:
                        throw new TransactionAssertionException(transaction.Id, "Unexpected state in transaction: " + transaction.State);
                }
            }
            catch (Exception ex)
            {
                return new HouseKeepTransactionResponse(transaction, HouseKeepTransactionAction.None, ex);
            }
        }

        private async Task<Transaction> ProcessOutstandingRequestsAsync(Transaction transaction, CancellationToken cancellationToken)
        {
            foreach (var request in transaction.Requests)
            {
                if (!await fullyAppliedRequestService.IsFullyAppliedAsync(new TransactionVersion(transaction.Id, request.Id), cancellationToken))
                {
                    var response = await ProcessRequestAsync(transaction, request.GetRequest(), request.Id, cancellationToken);
                    transaction = response.Transaction;
                }
            }

            return transaction;
        }

        private async Task<ImmutableDictionary<ItemKey, ItemTransactionState>> AcquireItemLocksAsync(
            Transaction transaction,
            AmazonDynamoDBRequest request,
            CancellationToken cancellationToken)
        {
            try
            {
                return await versionedItemStore.AcquireLocksAsync(transaction, request, cancellationToken);
            }
            catch (TransactionConflictedException ex)
            {
                await ProcessTransactionConflictAsync(transaction.GetId(), ex, cancellationToken);
            }

            // Re attempt after lock release
            return await versionedItemStore.AcquireLocksAsync(transaction, request, cancellationToken);
        }

        private async Task ProcessTransactionConflictAsync(TransactionId id, TransactionConflictedException ex, CancellationToken cancellationToken)
        {
            await Task.WhenAll(ex
                .ConflictingItems
                .GroupBy(v => v.TransactionStateValue.TransactionId ?? throw new InvalidOperationException("Expected non null transaction id"))
                .Select(async conflictingItemsByTransactionId =>
                {
                    try
                    {
                        // Attempt to rollback / commit the entire other transaction
                        var owningTransactionId = new TransactionId(conflictingItemsByTransactionId.Key);
                        await ResumeTransactionAsync(owningTransactionId, cancellationToken);
                        var owningTransaction = await transactionStore.GetAsync(owningTransactionId, true, cancellationToken);

                        // NOTE: Don't process an recently updated transaction as it would be half way through
                        if (!IsStale(owningTransaction))
                        {
                            return;
                        }

                        if (owningTransaction.State == TransactionState.Committing)
                        {
                            await CompleteTransactionAsync(owningTransactionId, false, cancellationToken);
                            return;
                        }

                        if (owningTransaction.State is TransactionState.RollingBack or TransactionState.Active)
                        {
                            await CompleteTransactionAsync(owningTransactionId, true, cancellationToken);
                            return;
                        }

                        return;
                    }
                    catch (TransactionCompletedException)
                    {
                        // Other transaction was already completed, carry on...
                    }
                    catch (TransactionNotFoundException)
                    {
                        // No transaction to rollback, release lock on individual item and carry on...
                        var owningTransaction = new TransactionId(conflictingItemsByTransactionId.Key);

                        var itemKeys = conflictingItemsByTransactionId
                            .Select(v => v.ItemKey)
                            .ToImmutableList();

                        var itemTransactionStatesByKey = conflictingItemsByTransactionId
                            .ToImmutableDictionary(
                                k => k.ItemKey,
                                v => ItemTransactionState.Create(v.ItemKey, v.TransactionStateValue, new LockedItemRequestAction(v.ItemKey, 1, RequestAction.Get)));

                        await versionedItemStore.ReleaseLocksAsync(
                            id,
                            owningTransaction,
                            itemKeys,
                            true,
                            itemTransactionStatesByKey,
                            ImmutableDictionary<ItemKey, ItemRecord>.Empty,
                            cancellationToken);
                    }
                }));
        }

        public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default)
        {
            return GetItemAsync(IsolationLevel.UnCommitted, request, cancellationToken);
        }

        public async Task<GetItemResponse> GetItemAsync(TransactionId id, GetItemRequest request, CancellationToken cancellationToken)
        {
            if (s_reRouteRequests)
            {
                var resp = await TransactGetItemsAsync(id, new TransactGetItemsRequest
                {
                    TransactItems = [
                        new() {
                            Get = new Get{
                                TableName = request.TableName,
                                Key = request.Key,
                                ProjectionExpression = request.ProjectionExpression,
                                ExpressionAttributeNames = request.ExpressionAttributeNames,
                            }
                        }
                    ]
                }, cancellationToken);

                return new GetItemResponse
                {
                    Item = resp.Responses.First().Item,
                    IsItemSet = resp.Responses.First().Item.Count > 0,
                };
            }

            var processResponse = await ProcessRequestAsync(id, request, cancellationToken);
            return (GetItemResponse)processResponse.AmazonWebServiceResponse;
        }

        public async Task<GetItemResponse> GetItemAsync(IsolationLevel isolationLevel, GetItemRequest request, CancellationToken cancellationToken = default)
        {
            ValidateRequest(request);
            return await GetIsolatedGetItemService(isolationLevel).GetItemAsync(request, cancellationToken);
        }

        public async Task<BatchGetItemResponse> BatchGetItemAsync(IsolationLevel isolationLevel, BatchGetItemRequest request, CancellationToken cancellationToken = default)
        {
            ValidateRequest(request);
            return await GetIsolatedGetItemService(isolationLevel).BatchGetItemAsync(request, cancellationToken);
        }

        public async Task<TransactGetItemsResponse> TransactGetItemsAsync(IsolationLevel isolationLevel, TransactGetItemsRequest request, CancellationToken cancellationToken = default)
        {
            ValidateRequest(request);
            return await GetIsolatedGetItemService(isolationLevel).TransactGetItemsAsync(request, cancellationToken);
        }

        public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.PutItemAsync(request, cancellationToken);
        }

        public async Task<PutItemResponse> PutItemAsync(TransactionId id, PutItemRequest request, CancellationToken cancellationToken = default)
        {
            if (s_reRouteRequests)
            {
                var resp = await ProcessRequestAsync(id, new TransactWriteItemsRequest
                {
                    TransactItems = [
                        new() {
                            Put = new Put{
                                TableName = request.TableName,
                                Item = request.Item,
                                ConditionExpression = request.ConditionExpression,
                                ExpressionAttributeNames = request.ExpressionAttributeNames,
                                ExpressionAttributeValues = request.ExpressionAttributeValues
                            }
                        }
                    ]
                }, cancellationToken);

                if (request.ReturnValues == null || request.ReturnValues == ReturnValue.NONE)
                {
                    return new PutItemResponse();
                }

                var itemTransactionState = resp.ItemTransactionStates.Values.Single();
                if (request.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
                {
                    return new PutItemResponse();
                }

                if (request.ReturnValues == ReturnValue.ALL_OLD)
                {
                    return new PutItemResponse
                    {
                        Attributes = resp.ItemsToBackupByKey.Values.Single().AttributeValues.ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                    };
                }

                throw new NotSupportedException();
            }

            var processResponse = await ProcessRequestAsync(id, request, cancellationToken);
            return (PutItemResponse)processResponse.AmazonWebServiceResponse;
        }

        public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateItemAsync(request, cancellationToken);
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(TransactionId id, UpdateItemRequest request, CancellationToken cancellationToken)
        {
            if (s_reRouteRequests)
            {
                var resp = await ProcessRequestAsync(id, new TransactWriteItemsRequest
                {
                    TransactItems = [
                        new() {
                            Update = new Update {
                                TableName = request.TableName,
                                Key = request.Key,
                                ConditionExpression = request.ConditionExpression,
                                UpdateExpression = request.UpdateExpression,
                                ExpressionAttributeNames = request.ExpressionAttributeNames,
                                ExpressionAttributeValues = request.ExpressionAttributeValues
                            }
                        }
                    ]
                }, cancellationToken);

                if (request.ReturnValues == null || request.ReturnValues == ReturnValue.NONE)
                {
                    return new UpdateItemResponse();
                }

                if (request.ReturnValues == ReturnValue.ALL_NEW)
                {
                    var getAllNewResponse = await amazonDynamoDB.GetItemAsync(new GetItemRequest
                    {
                        TableName = request.TableName,
                        Key = request.Key,
                        ConsistentRead = true,
                    }, cancellationToken);

                    return new UpdateItemResponse
                    {
                        Attributes = getAllNewResponse
                            .Item
                            .Where(i => !ItemAttributeName.Values.Contains(new ItemAttributeName(i.Key)))
                            .ToDictionary(k => k.Key, v => v.Value)
                    };
                }

                var itemTransactionState = resp.ItemTransactionStates.Values.Single();
                if (request.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
                {
                    return new UpdateItemResponse();
                }

                if (request.ReturnValues == ReturnValue.ALL_OLD)
                {
                    return new UpdateItemResponse
                    {
                        Attributes = resp
                            .ItemsToBackupByKey
                            .Values
                            .Single()
                            .AttributeValues
                            .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                    };
                }

                throw new NotSupportedException();
            }

            var processResponse = await ProcessRequestAsync(id, request, cancellationToken);
            return (UpdateItemResponse)processResponse.AmazonWebServiceResponse;
        }

        public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DeleteItemAsync(request, cancellationToken);
        }

        public async Task<DeleteItemResponse> DeleteItemAsync(TransactionId id, DeleteItemRequest request, CancellationToken cancellationToken)
        {
            if (s_reRouteRequests)
            {
                var resp = await ProcessRequestAsync(id, new TransactWriteItemsRequest
                {
                    TransactItems = [
                        new() {
                            Delete = new Delete{
                                TableName = request.TableName,
                                Key = request.Key,
                                ConditionExpression = request.ConditionExpression,
                                ExpressionAttributeNames = request.ExpressionAttributeNames,
                                ExpressionAttributeValues = request.ExpressionAttributeValues
                            }
                        }
                    ]
                }, cancellationToken);

                if (request.ReturnValues == null || request.ReturnValues == ReturnValue.NONE || request.ReturnValues == ReturnValue.ALL_NEW)
                {
                    return new DeleteItemResponse();
                }

                var itemTransactionState = resp.ItemTransactionStates.Values.Single();
                if (request.ReturnValues == ReturnValue.ALL_OLD && itemTransactionState.IsTransient)
                {
                    return new DeleteItemResponse();
                }

                if (request.ReturnValues == ReturnValue.ALL_OLD)
                {
                    return new DeleteItemResponse
                    {
                        Attributes = resp
                            .ItemsToBackupByKey
                            .Values
                            .Single()
                            .AttributeValues
                            .ToDictionary(k => k.Key, v => v.Value.ToAttributeValue())
                    };
                }

                throw new NotSupportedException();
            }

            var processResponse = await ProcessRequestAsync(id, request, cancellationToken);
            return (DeleteItemResponse)processResponse.AmazonWebServiceResponse;
        }

        public async Task<TransactGetItemsResponse> TransactGetItemsAsync(
            TransactionId id,
            TransactGetItemsRequest request,
            CancellationToken cancellationToken = default)
        {
            var processResponse = await ProcessRequestAsync(id, request, cancellationToken);
            return (TransactGetItemsResponse)processResponse.AmazonWebServiceResponse;
        }

        private bool IsStale(Transaction transaction)
        {
            return (transaction.LastUpdateDateTime + options.Value.TransactionStaleDuration) < DateTime.UtcNow;
        }

        private async Task<ApplyRequestResponse> ProcessRequestAsync(
           TransactionId id,
           AmazonDynamoDBRequest request,
           CancellationToken cancellationToken = default)
        {
            ValidateRequest(request);
            var transaction = await transactionStore.GetAsync(id, false, cancellationToken);
            switch (transaction.State)
            {
                case TransactionState.Active:
                    transaction = await Retry.RunAsync(async () =>
                    {
                        try
                        {
                            transaction = await ProcessTransactionAsync(transaction, true, cancellationToken);
                            await ValidateAppendRequestAsync(transaction, request, cancellationToken);
                            return await transactionStore.AppendRequestAsync(transaction, request, cancellationToken);
                        }
                        catch (TransactionException)
                        {
                            transaction = await transactionStore.GetAsync(transaction.GetId(), true, cancellationToken);
                            throw;
                        }
                    }, new RetryOptions
                    {
                        Condition = v => new RetryConditionResponse(
                            v.Exception is TransactionException && v.RetryAttempt < 3, TimeSpan.Zero)
                    }, cancellationToken);

                    return await Retry.RunAsync(async () =>
                    {
                        try
                        {
                            return await ProcessRequestAsync(transaction, request, transaction.Version, cancellationToken);
                        }
                        catch (TransactionException)
                        {
                            transaction = await transactionStore.GetAsync(transaction.GetId(), true, cancellationToken);
                            throw;
                        }
                    }, new RetryOptions
                    {
                        Condition = v => new RetryConditionResponse(v.Exception is TransactionException && v.RetryAttempt < 3, TimeSpan.Zero)
                    }, cancellationToken);
                case TransactionState.Committed:
                    throw new TransactionCommittedException(transaction.Id, "The transaction already committed");
                case TransactionState.RolledBack:
                    throw new TransactionRolledBackException(transaction.Id, "The transaction already rolled back");
                default:
                    throw new TransactionException(transaction.Id, "Unexpected state " + transaction.State);
            }
        }

        private async Task ValidateAppendRequestAsync(Transaction transaction, AmazonDynamoDBRequest request, CancellationToken cancellationToken)
        {
            var itemRequestGroups = await Task.WhenAll(transaction.Requests.Select(r => requestService.GetItemRequestDetailsAsync(r.GetRequest(), cancellationToken)));
            var existingItemRequestActions = itemRequestGroups.SelectMany(r => r).Aggregate(ImmutableDictionary<ItemKey, RequestAction>.Empty, (agg, cur) =>
            {
                if (!agg.TryGetValue(cur.Key, out var value))
                {
                    return agg.Add(cur.Key, cur.RequestAction);
                }

                if (value == RequestAction.Get)
                {
                    return agg.SetItem(cur.Key, cur.RequestAction);
                }

                throw new InvalidOperationException("Previously applied requests are invalid");
            });

            var newItemRequestActions = await requestService.GetItemRequestDetailsAsync(request, cancellationToken);
            foreach (var newItemRequestAction in newItemRequestActions)
            {
                if (newItemRequestAction.RequestAction == RequestAction.Get)
                {
                    // If this is just a get request then that is fine
                    continue;
                }

                if (!existingItemRequestActions.TryGetValue(newItemRequestAction.Key, out var existingItemRequestAction))
                {
                    // No existing request so that is fine
                    continue;
                }

                if (existingItemRequestAction == RequestAction.Get)
                {
                    // This is an upgrade from a read to a write which is fine
                    return;
                }

                throw new DuplicateRequestException();
            }
        }

        // NOTE AmazonDynamoDBRequest should be immutable because it is getting modified
        private async Task<ApplyRequestResponse> ProcessRequestAsync(
            Transaction transaction,
            AmazonDynamoDBRequest request,
            int requestId,
            CancellationToken cancellationToken)
        {
            var itemTransactionStatesByKey = await AcquireItemLocksAsync(transaction, request, cancellationToken);
            var itemsToBackup = await versionedItemStore.GetItemsToBackupAsync(request, cancellationToken);
            var itemsToBackupByKey = itemsToBackup.ToImmutableDictionary(k => k.Key);
            await itemImageStore.AddItemImagesAsync(new TransactionVersion(transaction.Id, requestId), itemsToBackup, cancellationToken);

            // Check transaction after acquiring locks and backing up images in case another process
            // has committed or rolled it back
            AmazonWebServiceResponse? response;
            try
            {
                transaction = await transactionStore.GetAsync(transaction.GetId(), true, cancellationToken);

                transaction = await ProcessTransactionAsync(transaction, false, cancellationToken);

                var applyRequestRequest = new ApplyRequestRequest(
                        transaction,
                        request,
                        requestId,
                        itemTransactionStatesByKey,
                        itemsToBackupByKey);

                response = await versionedItemStore.ApplyRequestAsync(
                    applyRequestRequest,
                    cancellationToken);

                switch (transaction.State)
                {
                    case TransactionState.Committing:
                    case TransactionState.Committed:
                        throw new TransactionCommittedException(transaction.Id);
                    case TransactionState.RollingBack:
                    case TransactionState.RolledBack:
                        throw new TransactionRolledBackException(transaction.Id);
                }
            }
            catch (Exception ex) when (ex is TransactionNotFoundException or TransactionCommittedException or TransactionRolledBackException)
            {
                // Revert the changes we just made
                var itemKeys = itemTransactionStatesByKey.Select(v => v.Key).ToImmutableList();
                var rollbackImages = await itemImageStore.GetItemImagesAsync(new TransactionVersion(transaction.Id, requestId), cancellationToken);
                var rollbackImagesByKey = rollbackImages.ToImmutableDictionary(k => k.Key);
                await versionedItemStore.ReleaseLocksAsync(
                    transaction.GetId(),
                    transaction.GetId(),
                    itemKeys,
                    true,
                    itemTransactionStatesByKey,
                    rollbackImagesByKey,
                    cancellationToken);

                throw;
            }

            var onUpdateFullyAppliedRequestsBeginAsync = transactionServiceEvents.OnUpdateFullyAppliedRequestsBeginAsync;
            if (onUpdateFullyAppliedRequestsBeginAsync != null)
            {
                await onUpdateFullyAppliedRequestsBeginAsync(transaction.GetVersion(), cancellationToken);
            }

            await fullyAppliedRequestService.SetFullyAppliedAsync(transaction.GetVersion(), cancellationToken);
            return new ApplyRequestResponse(
                transaction,
                response,
                itemTransactionStatesByKey,
                itemsToBackupByKey);
        }

        public async Task<TransactWriteItemsResponse> TransactWriteItemsAsync(
            TransactionId id,
            TransactWriteItemsRequest request,
            CancellationToken cancellationToken = default)
        {
            var response = await ProcessRequestAsync(id, request, cancellationToken);
            return (TransactWriteItemsResponse)response.AmazonWebServiceResponse;
        }

        private async Task<Transaction> ProcessTransactionAsync(Transaction transaction, bool processOutstandingRequests, CancellationToken cancellationToken)
        {
            switch (transaction.State)
            {
                case TransactionState.Active:
                    if (processOutstandingRequests)
                    {
                        await ProcessOutstandingRequestsAsync(transaction, cancellationToken);
                    }
                    return transaction;
                case TransactionState.Committing:
                    if (transactionServiceEvents.OnDoCommitBeginAsync != null)
                    {
                        var skip = await transactionServiceEvents.OnDoCommitBeginAsync(transaction.GetId(), cancellationToken);
                        if (skip)
                        {
                            return transaction;
                        }
                    }

                    await versionedItemStore.ReleaseLocksAsync(transaction, false, ImmutableDictionary<ItemKey, ItemRecord>.Empty, cancellationToken);
                    await itemImageStore.DeleteItemImagesAsync(transaction, cancellationToken);
                    return await transactionStore.UpdateAsync(transaction with
                    {
                        Version = transaction.Version + 1,
                        State = TransactionState.Committed
                    }, cancellationToken);
                case TransactionState.RollingBack:
                    if (transactionServiceEvents.OnDoRollbackBeginAsync != null)
                    {
                        await transactionServiceEvents.OnDoRollbackBeginAsync(transaction.GetId(), cancellationToken);
                    }

                    TransactionAssertionException.TxAssert(TransactionState.RollingBack == transaction.State, transaction.Id, "Transaction state is not " + TransactionState.RollingBack);

                    var rollbackImages = await GetItemImagesAsync(transaction, cancellationToken);
                    var rollbackImagesByKey = rollbackImages.ToImmutableDictionary(k => k.Key);
                    await versionedItemStore.ReleaseLocksAsync(transaction, true, rollbackImagesByKey, cancellationToken);
                    await itemImageStore.DeleteItemImagesAsync(transaction, cancellationToken);
                    return await transactionStore.UpdateAsync(transaction with
                    {
                        Version = transaction.Version + 1,
                        State = TransactionState.RolledBack
                    }, cancellationToken);
                default:
                    return transaction;
            }
        }

        public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.BatchGetItemAsync(request, cancellationToken);
        }

        public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.BatchWriteItemAsync(request, cancellationToken);
        }

        public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.QueryAsync(request, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ScanAsync(request, cancellationToken);
        }

        public async Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default)
        {
            // if (request.TransactItems.Count <= _options.Value.TransactionItemCountMaxValue)
            // {
            //     // We can shortcut the batched transactions because this group
            //     // of items fits within the standard dynamodb transaction limit
            //     var transactionId = new TransactionId("QUICK");
            //     var requestWithNoTransactionCondition = WithNoExistingTransactionCondition(request);
            //     try
            //     {
            //         return await InternalTransactWriteItemsAsync(transactionId, requestWithNoTransactionCondition, cancellationToken);
            //     }
            //     catch (TransactionConflictedException ex)
            //     {
            //         await ProcessTransactionConflictAsync(transactionId, ex, cancellationToken);
            //     }

            //     // Re attempt after lock release
            //     return await InternalTransactWriteItemsAsync(transactionId, requestWithNoTransactionCondition, cancellationToken);
            // }

            var transaction = await BeginTransactionAsync(cancellationToken);
            try
            {
                var locker = new SemaphoreSlim(1);
                var responses = await Task.WhenAll(request
                    .TransactItems
                    .ToListBatches(options.Value.TransactGetItemCountMaxValue)
                    .Select(async batch =>
                    {
                        await locker.WaitAsync(cancellationToken);
                        try
                        {
                            var batchTransactGetItemsRequest = new TransactGetItemsRequest { TransactItems = batch };
                            return await TransactGetItemsAsync(transaction, batchTransactGetItemsRequest, cancellationToken);
                        }
                        finally
                        {
                            locker.Release();
                        }
                    }));

                await CommitTransactionAsync(transaction, cancellationToken);
                return new TransactGetItemsResponse
                {
                    Responses = responses.SelectMany(r => r.Responses).ToList()
                };
            }
            catch (Exception)
            {
                await RollbackTransactionAsync(transaction, cancellationToken);
                throw;
            }
        }

        public async Task<TransactWriteItemsResponse> TransactWriteItemsAsync(
            TransactWriteItemsRequest request,
            CancellationToken cancellationToken = default)
        {
            if (options.Value.QuickTransactionsEnabled && request.TransactItems.Count <= options.Value.TransactWriteItemCountMaxValue)
            {
                // We can shortcut the batched transactions because this group
                // of items fits within the standard dynamodb transaction limit
                var transactionId = new TransactionId("QUICK");
                var requestWithNoTransactionCondition = WithNoExistingTransactionCondition(request);
                try
                {
                    return await InternalTransactWriteItemsAsync(transactionId, requestWithNoTransactionCondition, cancellationToken);
                }
                catch (TransactionConflictedException ex)
                {
                    await ProcessTransactionConflictAsync(transactionId, ex, cancellationToken);
                }

                // Re attempt after lock release
                return await InternalTransactWriteItemsAsync(transactionId, requestWithNoTransactionCondition, cancellationToken);
            }

            var transaction = await BeginTransactionAsync(cancellationToken);
            try
            {
                var transactionBatches = request
                    .TransactItems
                    .ToListBatches(options.Value.TransactWriteItemCountMaxValue);

                foreach (var transactionBatch in transactionBatches)
                {
                    var batchTransactWriteItemsRequest = new TransactWriteItemsRequest
                    {
                        TransactItems = transactionBatch
                    };

                    await TransactWriteItemsAsync(transaction, batchTransactWriteItemsRequest, cancellationToken);
                }

                await CommitTransactionAsync(transaction, cancellationToken);
                return new TransactWriteItemsResponse();
            }
            catch (Exception)
            {
                await RollbackTransactionAsync(transaction, cancellationToken);
                throw;
            }
        }

        private async Task<TransactWriteItemsResponse> InternalTransactWriteItemsAsync(
            TransactionId id,
            TransactWriteItemsRequest request,
            CancellationToken cancellationToken)
        {
            try
            {
                return await amazonDynamoDB.TransactWriteItemsAsync(request, cancellationToken);
            }
            catch (TransactionCanceledException ex)
            {
                var conflictedException = await TryGetTransactionConflictedExceptionAsync(
                    id, ex, request, cancellationToken);

                if (conflictedException != null)
                {
                    throw conflictedException;
                }

                throw;
            }
        }

        private async Task<TransactionConflictedException?> TryGetTransactionConflictedExceptionAsync(
            TransactionId id, TransactionCanceledException ex,
            AmazonDynamoDBRequest request, CancellationToken cancellationToken)
        {
            var itemRequestDetails = await requestService.GetItemRequestDetailsAsync(request, cancellationToken);

            var failedItems = ex
                .CancellationReasons
                .Select((c, i) =>
                {
                    if (c.Code == "ConditionalCheckFailed")
                    {
                        var itemKey = itemRequestDetails[i].Key;
                        return versionedItemStore.GetItemRecordAndTransactionState(itemKey, c.Item);
                    }

                    return null;
                })
                .WhereNotNull()
                .ToImmutableList();

            var failedItemsByKey = failedItems
                .ToImmutableDictionary(k => k.ItemResponse.Key);

            var conflictedItems = failedItems
                .Where(v => v.TransactionStateValue.TransactionId != null)
                .Select(v => new TransactionConflictItem(
                    v.ItemResponse.Key,
                    v.ItemResponse,
                    v.TransactionStateValue))
                .ToImmutableList();

            if (!conflictedItems.IsEmpty)
            {
                return new TransactionConflictedException(id.Id, conflictedItems);
            }

            return null;
        }

        private TransactWriteItemsRequest WithNoExistingTransactionCondition(TransactWriteItemsRequest request)
        {
            return new TransactWriteItemsRequest
            {
                TransactItems = request.TransactItems.Select(WithNoExistingTransactionCondition).ToList()
            };
        }

        private TransactWriteItem WithNoExistingTransactionCondition(TransactWriteItem transactWriteItem)
        {
            if (transactWriteItem.Delete != null)
            {
                return new TransactWriteItem
                {
                    Delete = new Delete
                    {
                        TableName = transactWriteItem.Delete.TableName,
                        Key = transactWriteItem.Delete.Key,
                        ConditionExpression = Combine(
                            transactWriteItem.Delete.ConditionExpression,
                            $"attribute_not_exists(#{ItemAttributeName.TXID.Value})",
                            $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                            $"attribute_not_exists(#{ItemAttributeName.TRANSIENT.Value})"),
                        ExpressionAttributeNames = transactWriteItem
                            .Delete
                            .ExpressionAttributeNames
                            .Concat([
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                            ])
                            .ToDictionary(k => k.Key, v => v.Value),
                        ExpressionAttributeValues = transactWriteItem.Delete.ExpressionAttributeValues,
                        ReturnValuesOnConditionCheckFailure = transactWriteItem.Delete.ReturnValuesOnConditionCheckFailure,
                    }
                };
            }

            if (transactWriteItem.ConditionCheck != null)
            {
                if (IsSupportedConditionExpression(transactWriteItem.ConditionCheck, "attribute_not_exists"))
                {
                    return new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = transactWriteItem.ConditionCheck.TableName,
                            Key = transactWriteItem.ConditionCheck.Key,
                            ConditionExpression = Combine(
                                $"attribute_not_exists(#{ItemAttributeName.TXID.Value})",
                                $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                                $"attribute_not_exists(#{ItemAttributeName.TRANSIENT.Value})"),
                            ExpressionAttributeNames = transactWriteItem
                                .ConditionCheck
                                .ExpressionAttributeNames
                                .Concat([
                                    new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                    new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                    new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                                 ])
                                .ToDictionary(k => k.Key, v => v.Value),
                            ReturnValuesOnConditionCheckFailure = transactWriteItem.ConditionCheck.ReturnValuesOnConditionCheckFailure,
                        }
                    };
                }

                throw new NotSupportedException("ConditionExpression format not supported");
            }

            if (transactWriteItem.Put != null)
            {
                return new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = transactWriteItem.Put.TableName,
                        Item = transactWriteItem.Put.Item,
                        ConditionExpression = Combine(
                            transactWriteItem.Put.ConditionExpression,
                            $"attribute_not_exists(#{ItemAttributeName.TXID.Value})",
                            $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                            $"attribute_not_exists(#{ItemAttributeName.TRANSIENT.Value})"),
                        ExpressionAttributeNames = transactWriteItem
                            .Put
                            .ExpressionAttributeNames
                            .Concat([
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                            ])
                            .ToDictionary(k => k.Key, v => v.Value),
                        ExpressionAttributeValues = transactWriteItem.Put.ExpressionAttributeValues,
                        ReturnValuesOnConditionCheckFailure = transactWriteItem.Put.ReturnValuesOnConditionCheckFailure,
                    }
                };
            }

            if (transactWriteItem.Update != null)
            {
                return new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = transactWriteItem.Update.TableName,
                        Key = transactWriteItem.Update.Key,
                        ConditionExpression = Combine(
                            transactWriteItem.Update.ConditionExpression,
                            $"attribute_not_exists(#{ItemAttributeName.TXID.Value})",
                            $"attribute_not_exists(#{ItemAttributeName.APPLIED.Value})",
                            $"attribute_not_exists(#{ItemAttributeName.TRANSIENT.Value})"),
                        UpdateExpression = transactWriteItem.Update.UpdateExpression,
                        ExpressionAttributeNames = transactWriteItem
                            .Update
                            .ExpressionAttributeNames
                            .Concat([
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TXID.Value}", ItemAttributeName.TXID.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.APPLIED.Value}", ItemAttributeName.APPLIED.Value),
                                new KeyValuePair<string, string>($"#{ItemAttributeName.TRANSIENT.Value}", ItemAttributeName.TRANSIENT.Value),
                            ])
                            .ToDictionary(k => k.Key, v => v.Value),
                        ExpressionAttributeValues = transactWriteItem.Update.ExpressionAttributeValues,
                        ReturnValuesOnConditionCheckFailure = transactWriteItem.Update.ReturnValuesOnConditionCheckFailure,
                    }
                };
            }

            throw new NotSupportedException();
        }

        private static bool IsSupportedConditionExpression(ConditionCheck conditionCheck, string conditionExpressionFunction)
        {
            return conditionCheck.Key.Keys.Any(key => conditionCheck.ConditionExpression == $"{conditionExpressionFunction}({key})");
        }

        private static string Combine(params string?[] expressions)
        {
            return string.Join(" AND ", expressions.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e?.Trim()));
        }

        public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.BatchExecuteStatementAsync(request, cancellationToken);
        }

        public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ExecuteStatementAsync(request, cancellationToken);
        }

        public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ExecuteTransactionAsync(request, cancellationToken);
        }

        public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default)
        {
            return BatchGetItemAsync(new BatchGetItemRequest { RequestItems = requestItems, ReturnConsumedCapacity = returnConsumedCapacity }, cancellationToken);
        }

        public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default)
        {
            return BatchGetItemAsync(new BatchGetItemRequest { RequestItems = requestItems }, cancellationToken);
        }

        public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default)
        {
            return BatchWriteItemAsync(new BatchWriteItemRequest { RequestItems = requestItems }, cancellationToken);
        }

        public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default)
        {
            return DeleteItemAsync(new DeleteItemRequest { TableName = tableName, Key = key }, cancellationToken);
        }

        public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues, CancellationToken cancellationToken = default)
        {
            return DeleteItemAsync(new DeleteItemRequest { TableName = tableName, Key = key, ReturnValues = returnValues }, cancellationToken);
        }

        public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default)
        {
            return GetItemAsync(new GetItemRequest { TableName = tableName, Key = key }, cancellationToken);
        }

        public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default)
        {
            return GetItemAsync(new GetItemRequest { TableName = tableName, Key = key, ConsistentRead = consistentRead }, cancellationToken);
        }

        public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default)
        {
            return PutItemAsync(new PutItemRequest { TableName = tableName, Item = item }, cancellationToken);
        }

        public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = default)
        {
            return PutItemAsync(new PutItemRequest { TableName = tableName, Item = item, ReturnValues = returnValues }, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default)
        {
            return ScanAsync(new ScanRequest { TableName = tableName, AttributesToGet = attributesToGet }, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default)
        {
            return ScanAsync(new ScanRequest { TableName = tableName, ScanFilter = scanFilter }, cancellationToken);
        }

        public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default)
        {
            return ScanAsync(new ScanRequest { TableName = tableName, AttributesToGet = attributesToGet, ScanFilter = scanFilter }, cancellationToken);
        }

        public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default)
        {
            return UpdateItemAsync(new UpdateItemRequest { TableName = tableName, Key = key, AttributeUpdates = attributeUpdates }, cancellationToken);
        }

        public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default)
        {
            return UpdateItemAsync(new UpdateItemRequest { TableName = tableName, Key = key, AttributeUpdates = attributeUpdates, ReturnValues = returnValues }, cancellationToken);
        }

        public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.CreateBackupAsync(request, cancellationToken);
        }

        public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.CreateGlobalTableAsync(request, cancellationToken);
        }

        public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.CreateTableAsync(tableName, keySchema, attributeDefinitions, provisionedThroughput, cancellationToken);
        }

        public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.CreateTableAsync(request, cancellationToken);
        }

        public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DeleteBackupAsync(request, cancellationToken);
        }

        public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DeleteTableAsync(tableName, cancellationToken);
        }

        public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DeleteTableAsync(request, cancellationToken);
        }

        public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeBackupAsync(request, cancellationToken);
        }

        public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeContinuousBackupsAsync(request, cancellationToken);
        }

        public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeContributorInsightsAsync(request, cancellationToken);
        }

        public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeEndpointsAsync(request, cancellationToken);
        }

        public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeExportAsync(request, cancellationToken);
        }

        public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeGlobalTableAsync(request, cancellationToken);
        }

        public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeGlobalTableSettingsAsync(request, cancellationToken);
        }

        public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeKinesisStreamingDestinationAsync(request, cancellationToken);
        }

        public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeLimitsAsync(request, cancellationToken);
        }

        public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeTableAsync(tableName, cancellationToken);
        }

        public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeTableAsync(request, cancellationToken);
        }

        public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeTableReplicaAutoScalingAsync(request, cancellationToken);
        }

        public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeTimeToLiveAsync(tableName, cancellationToken);
        }

        public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeTimeToLiveAsync(request, cancellationToken);
        }

        public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DisableKinesisStreamingDestinationAsync(request, cancellationToken);
        }

        public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.EnableKinesisStreamingDestinationAsync(request, cancellationToken);
        }

        public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ExportTableToPointInTimeAsync(request, cancellationToken);
        }

        public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListBackupsAsync(request, cancellationToken);
        }

        public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListContributorInsightsAsync(request, cancellationToken);
        }

        public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListExportsAsync(request, cancellationToken);
        }

        public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListGlobalTablesAsync(request, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListTablesAsync(cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListTablesAsync(exclusiveStartTableName, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListTablesAsync(exclusiveStartTableName, limit, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListTablesAsync(limit, cancellationToken);
        }

        public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListTablesAsync(request, cancellationToken);
        }

        public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListTagsOfResourceAsync(request, cancellationToken);
        }

        public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.RestoreTableFromBackupAsync(request, cancellationToken);
        }

        public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.RestoreTableToPointInTimeAsync(request, cancellationToken);
        }

        public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.TagResourceAsync(request, cancellationToken);
        }

        public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UntagResourceAsync(request, cancellationToken);
        }

        public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateContinuousBackupsAsync(request, cancellationToken);
        }

        public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateContributorInsightsAsync(request, cancellationToken);
        }

        public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateGlobalTableAsync(request, cancellationToken);
        }

        public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateGlobalTableSettingsAsync(request, cancellationToken);
        }

        public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateTableAsync(tableName, provisionedThroughput, cancellationToken);
        }

        public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateTableAsync(request, cancellationToken);
        }

        public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateTableReplicaAutoScalingAsync(request, cancellationToken);
        }

        public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateTimeToLiveAsync(request, cancellationToken);
        }

        public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DescribeImportAsync(request, cancellationToken);
        }

        public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ImportTableAsync(request, cancellationToken);
        }

        public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.ListImportsAsync(request, cancellationToken);
        }

        public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request)
        {
            return amazonDynamoDB.DetermineServiceOperationEndpoint(request);
        }

        public void Dispose()
        {
        }

        private static void ValidateRequest(AmazonDynamoDBRequest request)
        {
            switch (request)
            {
                case GetItemRequest getItemRequest:
                    ValidateRequest(getItemRequest);
                    return;
                case PutItemRequest putItemRequest:
                    ValidateRequest(putItemRequest);
                    return;
                case UpdateItemRequest updateItemRequest:
                    ValidateRequest(updateItemRequest);
                    return;
                case DeleteItemRequest deleteItemRequest:
                    ValidateRequest(deleteItemRequest);
                    return;
                case TransactWriteItemsRequest transactWriteItemsRequest:
                    ValidateRequest(transactWriteItemsRequest);
                    return;
                case TransactGetItemsRequest transactGetItemsRequest:
                    ValidateRequest(transactGetItemsRequest);
                    return;
                default:
                    throw new NotSupportedException();
            }
        }

        private static void ValidateRequest(GetItemRequest request)
        {
            if (request.AttributesToGet?.Count > 0)
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }

            if (request.Key?.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (!string.IsNullOrWhiteSpace(request.ProjectionExpression) && ItemAttributeName.Values.Where(v => request.ProjectionExpression.Contains(v.Value)).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (request.ExpressionAttributeNames != null && request.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }
        }

        private static void ValidateRequest(PutItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Item.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (!string.IsNullOrWhiteSpace(request.ConditionExpression) && ItemAttributeName.Values.Where(v => request.ConditionExpression.Contains(v.Value)).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (request.ExpressionAttributeNames != null && request.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }
        }

        private static void ValidateRequest(UpdateItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0) || (request.AttributeUpdates?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }

            if (request.Key?.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (!string.IsNullOrWhiteSpace(request.ConditionExpression) && ItemAttributeName.Values.Where(v => request.ConditionExpression.Contains(v.Value)).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (!string.IsNullOrWhiteSpace(request.UpdateExpression) && ItemAttributeName.Values.Where(v => request.UpdateExpression.Contains(v.Value)).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (request.ExpressionAttributeNames != null && request.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }
        }

        private static void ValidateRequest(DeleteItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }

            if (request.Key?.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (!string.IsNullOrWhiteSpace(request.ConditionExpression) && ItemAttributeName.Values.Where(v => request.ConditionExpression.Contains(v.Value)).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }

            if (request.ExpressionAttributeNames != null && request.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }
        }

        private static void ValidateRequest(TransactGetItemsRequest request)
        {
            if (request.TransactItems != null && request.TransactItems.Where(v => string.IsNullOrWhiteSpace(v.Get.TableName)).Any())
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.TransactItems != null && request.TransactItems.Where(v => v.Get.Key?.Count == 0).Any())
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }

            if (request.TransactItems != null && request.TransactItems.Where(HasReservedAttribute).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }
        }

        private static void ValidateRequest(TransactWriteItemsRequest request)
        {
            if (request.TransactItems != null && request.TransactItems.Where(v => string.IsNullOrWhiteSpace(v.ConditionCheck?.TableName ?? v.Delete?.TableName ?? v.Put?.TableName ?? v.Update?.TableName)).Any())
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.TransactItems != null && request.TransactItems.Where(IsKeyNull).Any())
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }

            if (request.TransactItems != null && request.TransactItems.Where(HasReservedAttribute).Any())
            {
                throw new InvalidOperationException("Request must not contain a reserved attribute");
            }
        }

        private static bool IsKeyNull(TransactWriteItem value)
        {
            if (value.ConditionCheck != null)
            {
                return value.ConditionCheck.Key?.Count == 0;
            }

            if (value.Delete != null)
            {
                return value.Delete.Key?.Count == 0;
            }

            if (value.Put != null)
            {
                return false;
            }

            if (value.Update != null)
            {
                return value.Update.Key?.Count == 0;
            }

            throw new NotSupportedException();
        }

        private static bool HasReservedAttribute(TransactGetItem value)
        {
            if (value.Get?.Key.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
            {
                return true;
            }

            if (!string.IsNullOrWhiteSpace(value.Get?.ProjectionExpression) && ItemAttributeName.Values.Where(v => value.Get?.ProjectionExpression.Contains(v.Value) ?? false).Any())
            {
                return true;
            }

            if (value.Get?.ExpressionAttributeNames != null && value.Get.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
            {
                return true;
            }

            return false;
        }

        private static bool HasReservedAttribute(TransactWriteItem value)
        {
            if (value.ConditionCheck != null)
            {
                if (value.ConditionCheck.Key?.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
                {
                    return true;
                }

                if (!string.IsNullOrWhiteSpace(value.ConditionCheck.ConditionExpression) && ItemAttributeName.Values.Where(v => value.ConditionCheck.ConditionExpression.Contains(v.Value)).Any())
                {
                    return true;
                }

                if (value.ConditionCheck.ExpressionAttributeNames != null && value.ConditionCheck.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
                {
                    return true;
                }

                return false;
            }

            if (value.Delete != null)
            {
                if (value.Delete.Key?.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
                {
                    return true;
                }

                if (!string.IsNullOrWhiteSpace(value.Delete.ConditionExpression) && ItemAttributeName.Values.Where(v => value.Delete.ConditionExpression.Contains(v.Value)).Any())
                {
                    return true;
                }

                if (value.Delete.ExpressionAttributeNames != null && value.Delete.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
                {
                    return true;
                }

                return false;
            }

            if (value.Put != null)
            {
                if (value.Put.Item.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any())
                {
                    return true;
                }

                if (!string.IsNullOrWhiteSpace(value.Put.ConditionExpression) && ItemAttributeName.Values.Where(v => value.Put.ConditionExpression.Contains(v.Value)).Any())
                {
                    return true;
                }

                if (value.Put.ExpressionAttributeNames != null && value.Put.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
                {
                    return true;
                }

                return false;
            }

            if (value.Update != null)
            {
                if (value.Update.Key?.Where(k => ItemAttributeName.Values.Contains(new ItemAttributeName(k.Key))).Any() ?? false)
                {
                    return true;
                }

                if (!string.IsNullOrWhiteSpace(value.Update.ConditionExpression) && ItemAttributeName.Values.Where(v => value.Update.ConditionExpression.Contains(v.Value)).Any())
                {
                    return true;
                }

                if (value.Update.ExpressionAttributeNames != null && value.Update.ExpressionAttributeNames.Values.Where(v => ItemAttributeName.Values.Contains(new ItemAttributeName(v))).Any())
                {
                    return true;
                }

                return false;
            }

            throw new NotSupportedException();
        }

        private async Task<ImmutableList<ItemRecord>> GetItemImagesAsync(Transaction transaction, CancellationToken cancellationToken)
        {
            // Get the first saved image for each item in the transaction
            var itemImageGroups = await Task.WhenAll(transaction
                .Requests
                .Select(request => itemImageStore.GetItemImagesAsync(new TransactionVersion(transaction.Id, request.Id), cancellationToken)));

            return itemImageGroups
                .SelectMany(v => v)
                .GroupBy(v => v.Key)
                .Select(v => v.First())
                .ToImmutableList();
        }

        public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.DeleteResourcePolicyAsync(request, cancellationToken);
        }

        public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.GetResourcePolicyAsync(request, cancellationToken);
        }

        public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.PutResourcePolicyAsync(request, cancellationToken);
        }

        public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default)
        {
            return amazonDynamoDB.UpdateKinesisStreamingDestinationAsync(request, cancellationToken);
        }
    }
}