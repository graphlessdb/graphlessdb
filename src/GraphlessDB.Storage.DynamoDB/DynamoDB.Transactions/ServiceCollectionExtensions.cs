/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB.DynamoDB.Transactions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddDynamoDBTransactionsCore(
            this IServiceCollection source)
        {
            return
                source
                .AddScoped<IAmazonDynamoDBWithTransactions, AmazonDynamoDBWithTransactions>()
                .AddScoped<ITransactionServiceEvents, TransactionServiceEvents>()
                .AddScoped<IIsolatedGetItemService<CommittedIsolationLevelServiceType>, CommittedIsolatedGetItemService>()
                .AddScoped<IIsolatedGetItemService<UnCommittedIsolationLevelServiceType>, UnCommittedIsolatedGetItemService>()
                .AddScoped<IRequestService, RequestService>()
                .AddScoped<IRequestRecordSerializer, RequestRecordSerializer>()
                .AddScoped<IVersionedItemStore, VersionedItemStore>()
                .AddScoped<IAmazonDynamoDBKeyService, AmazonDynamoDBKeyService>()
                .AddScoped<IFullyAppliedRequestService, InMemoryFullyAppliedRequestService>()
                .AddScoped<ITableSchemaService, TableSchemaService>();
        }

        public static IServiceCollection AddDynamoDBTransactionsWithDefaultStorage(
            this IServiceCollection source)
        {
            return source
                .AddDynamoDBTransactionsCore()
                .AddScoped<ITransactionStore, DefaultTransactionStore>()
                .AddScoped<IItemImageStore, DefaultItemImageStore>();
        }
    }
}
