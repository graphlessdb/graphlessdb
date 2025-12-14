/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions
{
    public interface IAmazonDynamoDBWithTransactions : IAmazonDynamoDB
    {
        Task<TransactionId> BeginTransactionAsync(CancellationToken cancellationToken = default);

        Task<TransactionId> ResumeTransactionAsync(TransactionId id, CancellationToken cancellationToken = default);

        Task CommitTransactionAsync(TransactionId id, CancellationToken cancellationToken = default);

        Task RollbackTransactionAsync(TransactionId id, CancellationToken cancellationToken = default);

        Task<GetItemResponse> GetItemAsync(IsolationLevel isolationLevel, GetItemRequest request, CancellationToken cancellationToken = default);

        Task<BatchGetItemResponse> BatchGetItemAsync(IsolationLevel isolationLevel, BatchGetItemRequest request, CancellationToken cancellationToken = default);

        Task<TransactGetItemsResponse> TransactGetItemsAsync(IsolationLevel isolationLevel, TransactGetItemsRequest request, CancellationToken cancellationToken = default);

        Task<GetItemResponse> GetItemAsync(TransactionId id, GetItemRequest request, CancellationToken cancellationToken = default);

        Task<PutItemResponse> PutItemAsync(TransactionId id, PutItemRequest request, CancellationToken cancellationToken = default);

        Task<UpdateItemResponse> UpdateItemAsync(TransactionId id, UpdateItemRequest request, CancellationToken cancellationToken = default);

        Task<DeleteItemResponse> DeleteItemAsync(TransactionId id, DeleteItemRequest request, CancellationToken cancellationToken = default);

        Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactionId id, TransactGetItemsRequest request, CancellationToken cancellationToken = default);

        Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactionId id, TransactWriteItemsRequest request, CancellationToken cancellationToken = default);

        Task<RunHouseKeepingResponse> RunHouseKeepingAsync(RunHouseKeepingRequest request, CancellationToken cancellationToken);
    }
}
