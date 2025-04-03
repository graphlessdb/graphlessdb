/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using GraphlessDB.Storage.Services.DynamoDB;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddGraphlessDBWithDynamoDB(
            this IServiceCollection source)
        {
            return source
                .AddGraphlessDBCore()
                .AddScoped<IAmazonDynamoDBRDFTripleItemService, AmazonDynamoDBRDFTripleItemService>()
                .AddScoped<IRDFTripleIntegrityChecker, AmazonDynamoDBRDFTripleIntegrityChecker>()
                .AddScoped<IRDFTripleStore<StoreType.Data>, AmazonDynamoDBRDFTripleStore>();
        }

        public static IServiceCollection AddAmazonDynamoDBOptions(
            this IServiceCollection source, Action<AmazonDynamoDBOptions> configureOptions)
        {
            source
                .AddOptions<AmazonDynamoDBOptions>()
                .Configure(configureOptions);

            return source;
        }
    }
}
