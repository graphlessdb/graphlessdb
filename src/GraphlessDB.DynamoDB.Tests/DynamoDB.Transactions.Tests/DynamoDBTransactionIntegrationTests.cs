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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using GraphlessDB;
using GraphlessDB.DynamoDB.Transactions.Internal;
using GraphlessDB.DynamoDB.Transactions.Storage;
using GraphlessDB.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    public class DynamoDBTransactionIntegrationTests
    {
        public sealed record SetupData(ImmutableDictionary<string, AttributeValue> Key0, ImmutableDictionary<string, AttributeValue> Item0);

        public async Task CanLockItemAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var testDynamoDBService1 = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var testDynamoDBService2 = scope2.ServiceProvider.GetRequiredService<ITestDynamoDBService>();


            var key1 = testDynamoDBService1.NewKey();
            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var t2 = await client2.BeginTransactionAsync(cancellationToken);

            var lockRequest = new GetItemRequest
            {
                TableName = testDynamoDBService1.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            };

            var getResult = await client1.GetItemAsync(t1, lockRequest, cancellationToken);

            // we're not applying locks
            await assertService1.AssertItemLockedAsync(testDynamoDBService1.GetTableName(), key1, t1.Id, true, false, cancellationToken);
            Assert.IsFalse(getResult.IsItemSet);

            var deleteRequest = new DeleteItemRequest
            {
                TableName = testDynamoDBService2.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            };

            var deleteResult = await client2.DeleteItemAsync(t2, deleteRequest, cancellationToken);

            // we're not applying deletes either
            await assertService2.AssertItemLockedAsync(testDynamoDBService2.GetTableName(), key1, t2.Id, true, false, cancellationToken);

            // return values is null in the request
            Assert.IsFalse(deleteResult.Attributes.Count > 0);

            await client2.CommitTransactionAsync(t2, cancellationToken);

            try
            {
                await client1.CommitTransactionAsync(t1, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException)
            {

            }

            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService1.GetTableName(), key1, false, cancellationToken);
        }

        public async Task CanLock2ItemsAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var testDynamoDBService0 = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var testDynamoDBService1 = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();


            var key1 = testDynamoDBService0.NewKey();
            var key2 = testDynamoDBService1.NewKey();

            var t0 = await transactionService0.BeginTransactionAsync(cancellationToken);
            var item1 = key1.Add("something", AttributeValueFactory.CreateS("val"));

            var putResult = await transactionService0.PutItemAsync(t0, new PutItemRequest
            {
                TableName = testDynamoDBService0.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD
            }, cancellationToken);

            Assert.IsTrue(putResult.Attributes.Count == 0);

            await transactionService0.CommitTransactionAsync(t0, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var getResult1 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService1.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService1.GetTableName(), key1, item1, t1.Id, false, false, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(item1, getResult1.Item));

            var getResult2 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService1.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService1.GetTableName(), key1, item1, t1.Id, false, false, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService1.GetTableName(), key2, t1.Id, true, false, cancellationToken);
            Assert.IsTrue(!getResult2.IsItemSet);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService1.GetTableName(), key1, item1, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService1.GetTableName(), key2, false, cancellationToken);
        }

        public async Task CanGetItemWithDeleteAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            // Get Obj0 using Trans1
            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var getResult1 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(TestHelpers.AreEqual(getResult1.Item, item0));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            // Delete Obj0 using Trans1
            await client1.DeleteItemAsync(t1, new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            // Try Get Obj0 using Trans1 and fail
            var getResult2 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            Assert.IsTrue(!getResult2.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
        }

        public async Task GetFilterAttributesToGetAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var item1 = ImmutableDictionary<string, AttributeValue>
                .Empty
                .Add("s_someattr", item0["s_someattr"]);

            var getResult1 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                ProjectionExpression = "s_someattr, notexists",
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(TestHelpers.AreEqual(item1, getResult1.Item));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, t1.Id, false, false, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item0, true, cancellationToken);
        }

        public async Task GetItemNotExistsAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();

            var getResult1 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(!getResult1.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);

            var getResult2 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            Assert.IsTrue(!getResult2.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
        }

        public async Task GetItemAfterPutItemInsertAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var item1 = key1
                .Add("asdf", AttributeValueFactory.CreateS("wef"));

            var getResult1 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(!getResult1.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);

            var putResult1 = await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            Assert.IsTrue(putResult1.Attributes.Count == 0);

            var getResult2 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(TestHelpers.AreEqual(getResult2.Item, item1));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task GetItemAfterPutItemOverwriteAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var item1 = item0
               .Add("asdf", AttributeValueFactory.CreateS("wef"));

            var getResult1 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(TestHelpers.AreEqual(getResult1.Item, item0));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            var putResult1 = await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(putResult1.Attributes, item0));

            var getResult2 = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(TestHelpers.AreEqual(getResult2.Item, item1));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item1, true, cancellationToken);
        }

        public async Task GetItemAfterPutItemInsertInResumedTxAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            // Simulate an error during the apply phase for transaction 1
            var thrown = false;
            transactionServiceEvents1.OnApplyRequestAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown)
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            var t2 = await client1.ResumeTransactionAsync(t1, cancellationToken);

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            try
            {
                // This Put needs to fail in apply
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item1.ToDictionary(k => k.Key, v => v.Value),
                    ReturnValues = ReturnValue.ALL_OLD,
                }, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);

            // second copy of same transaction
            var getResult1 = await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            Assert.IsTrue(TestHelpers.AreEqual(getResult1.Item, item1));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);

            await client2.CommitTransactionAsync(t2, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task GetItemThenPutItemInResumedTxThenGetItemAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            // Simulate an error during the apply phase for transaction 1
            var thrown = false;
            transactionServiceEvents1.OnApplyRequestAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown && (r is TransactWriteItemsRequest || r is PutItemRequest))
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            var t2 = await client1.ResumeTransactionAsync(t1, cancellationToken);

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            // Get a read lock in t2
            var getResult1 = await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            Assert.IsTrue(!getResult1.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, null, t1.Id, true, false, cancellationToken);

            // Begin a PutItem in t1, but fail apply
            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item1.ToDictionary(k => k.Key, v => v.Value),
                    ReturnValues = ReturnValue.ALL_OLD
                }, cancellationToken);

                Assert.Fail();
            }
            catch (FailedYourRequestException)
            {

            }
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);

            // Read again in the non-failing copy of the transaction
            var getResult2 = await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await client2.CommitTransactionAsync(t2, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(item1, getResult2.Item));

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task GetThenUpdateNewItemAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("didn't exist"));

            var getResult = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);
            Assert.IsTrue(!getResult.IsItemSet);

            var updateResult = await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = $"SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>{
                    { ":asdf", AttributeValueFactory.CreateS("didn't exist") }
                },
                ReturnValues = ReturnValue.ALL_NEW
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(item1, updateResult.Attributes));

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task GetThenUpdateExistingItemAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var item0a = item0.Add("wef", AttributeValueFactory.CreateS("new attr"));

            var getResult = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(item0, getResult.Item));

            var updateResult = await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET wef = :wef",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":wef", AttributeValueFactory.CreateS("new attr")}
                },
                ReturnValues = ReturnValue.ALL_NEW
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0a, t1.Id, false, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(item0a, updateResult.Attributes));

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item0a, true, cancellationToken);
        }

        public async Task GetItemUncommittedInsertAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.UnCommitted,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key1.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item1, getItemResponse.Item.ToImmutableDictionary()));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);

            await client1.RollbackTransactionAsync(t1, cancellationToken);
        }

        public async Task GetItemUncommittedDeleted(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            await client1.DeleteItemAsync(t1, new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.UnCommitted,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key0.ToDictionary(k => k.Key, v => v.Value)
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item0, getItemResponse.Item));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            await client1.RollbackTransactionAsync(t1, cancellationToken);
        }

        public async Task GetItemCommittedInsert(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.Committed,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key1.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            Assert.IsTrue(!getItemResponse.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);

            await client1.RollbackTransactionAsync(t1, cancellationToken);
        }

        public async Task GetItemCommittedDeleted(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            await client1.DeleteItemAsync(t1, new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.Committed,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key0.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item0, getItemResponse.Item));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            await client1.RollbackTransactionAsync(t1, cancellationToken);
        }

        public async Task GetItemCommittedUpdated(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("asdf"));

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("asdf")}
                },
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.Committed,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key0.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item0, getItemResponse.Item));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            getItemResponse = await client1.GetItemAsync(
                IsolationLevel.Committed,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key0.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item1, getItemResponse.Item));
        }

        public async Task GetItemCommittedUpdatedAndApplied(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnDoCommitBeginAsync = async (TransactionId t, CancellationToken c) =>
            {
                // Return true to skip a commit if the transaction is T1
                // This will skip cleaning up the transaction so we can validate reading.
                await Task.CompletedTask;
                if (!thrown)
                {
                    thrown = true;
                    return t.Id == t1.Id;
                }

                return false;
            };

            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("asdf"));

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("asdf")}
                },
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.Committed,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key0.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item1, getItemResponse.Item));
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);
        }

        public async Task GetItemCommittedMissingImage(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("asdf"));

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("asdf") }
                },
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            var graphOptions = serviceProvider.GetRequiredService<IOptions<GraphOptions>>();
            var failingAmazonDynamoDBOptions = serviceProvider.GetRequiredService<IOptions<FailingAmazonDynamoDBOptions>>();
            var itemImageStore = scope1.ServiceProvider.GetRequiredService<IItemImageStore>();
            failingAmazonDynamoDBOptions
                .Value
                .GetRequestsToTreatAsDeleted
                .Add(new GetItemRequest
                {
                    TableName = graphOptions.Value.TableName,
                    Key = itemImageStore.GetKey(new TransactionVersion(t1.Id, 2)),
                    ConsistentRead = true
                });

            try
            {
                await client1.GetItemAsync(
                    IsolationLevel.Committed,
                    new GetItemRequest
                    {
                        TableName = testDynamoDBService.GetTableName(),
                        Key = key0.ToDictionary(k => k.Key, v => v.Value),
                    }, cancellationToken);
                Assert.Fail("Should have thrown an exception.");
            }
            catch (TransactionException e)
            {
                Assert.AreEqual("Ran out of attempts to get a committed image of the item", e.Message);
            }
            catch (Exception e)
            {
                Assert.Fail($"Should have thrown a TransactionException, threw '{e.GetType().Name}'.");
            }
        }

        public async Task GetItemCommittedConcurrentCommit(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            // Test reading an item while simulating another transaction committing concurrently.
            // To do this we skip cleanup, make the item image appear to be deleted,
            // and then make the reader get the uncommitted version of the transaction 
            // row for the first read and then actual updated version for later reads.

            var graphOptions = serviceProvider.GetRequiredService<IOptions<GraphOptions>>();
            var failingAmazonDynamoDBOptions = serviceProvider.GetRequiredService<IOptions<FailingAmazonDynamoDBOptions>>();

            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();
            var itemImageStore0 = scope0.ServiceProvider.GetRequiredService<IItemImageStore>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var dynamoDB = serviceProvider.GetRequiredService<IAmazonDynamoDB>();
            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnDoCommitBeginAsync = async (TransactionId t, CancellationToken c) =>
            {
                // Return true to skip a commit if the transaction is T1
                // This will skip cleaning up the transaction so we can validate reading.
                await Task.CompletedTask;
                if (!thrown)
                {
                    thrown = true;
                    return t.Id == t1.Id;
                }

                return false;
            };

            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("asdf"));

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("asdf")}
                },
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            var transactionRequest = new GetItemRequest
            {
                TableName = graphOptions.Value.TableName,
                Key = transactionStore1.GetKey(t1),
                ConsistentRead = true
            };

            // Save the copy of the transaction before commit. 
            var uncommittedTransaction = await dynamoDB.GetItemAsync(transactionRequest, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);

            failingAmazonDynamoDBOptions
                .Value
                .GetRequestsToStub
                .Add(transactionRequest, new Queue<GetItemResponse>([uncommittedTransaction]));

            // Stub out the image so it appears deleted
            failingAmazonDynamoDBOptions
                .Value
                .GetRequestsToTreatAsDeleted
                .Add(new GetItemRequest
                {
                    TableName = graphOptions.Value.TableName,
                    Key = itemImageStore0.GetKey(new TransactionVersion(t1.Id, 1)),
                    ConsistentRead = true
                });

            var getItemResponse = await client1.GetItemAsync(
                IsolationLevel.Committed,
                new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key0.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
            assertService1.AssertNoSpecialAttributes(getItemResponse.Item.ToImmutableDictionary());
            Assert.IsTrue(TestHelpers.AreEqual(item1, getItemResponse.Item));
        }

        /*
        * ReturnValues tests
        */
        public async Task PutItemAllOldInsert(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            var putResult1 = await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            Assert.IsTrue(putResult1.Attributes.Count == 0);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task PutItemAllOldOverwrite(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("wef"));

            var putResult1 = await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(putResult1.Attributes, item0));

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item1, true, cancellationToken);
        }

        public async Task UpdateItemAllOldInsert(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            var result1 = await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("wef")}
                },
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            Assert.IsTrue(result1.Attributes.Count == 0);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task UpdateItemAllOldOverwrite(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("wef"));

            var result1 = await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("wef")}
                },
                ReturnValues = ReturnValue.ALL_OLD
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(result1.Attributes, item0));

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item1, true, cancellationToken);
        }

        public async Task UpdateItemAllNewInsert(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("asdf", AttributeValueFactory.CreateS("wef"));

            var result1 = await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("wef")}
                },
                ReturnValues = ReturnValue.ALL_NEW
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(result1.Attributes, item1));

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
        }

        public async Task UpdateItemAllNewOverwrite(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var item1 = item0.Add("asdf", AttributeValueFactory.CreateS("wef"));

            var result1 = await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("wef")}
                },
                ReturnValues = ReturnValue.ALL_NEW
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item1, t1.Id, false, true, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(result1.Attributes, item1));

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item1, true, cancellationToken);
        }

        public async Task DeleteItemAllOldNotExists(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();

            var result1 = await client1.DeleteItemAsync(t1, new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, key1, t1.Id, true, false, cancellationToken);
            Assert.IsTrue(result1.Attributes.Count == 0);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
        }

        public async Task DeleteItemAllOldExists(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var result1 = await client1.DeleteItemAsync(t1, new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key0.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD,
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);
            Assert.IsTrue(TestHelpers.AreEqual(item0, result1.Attributes));

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, false, cancellationToken);
        }

        /*
        * Transaction isolation and error tests
        */
        public async Task ConflictingWrites(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService3 = scope3.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var key1 = testDynamoDBService.NewKey();
            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var t2 = await client2.BeginTransactionAsync(cancellationToken);
            var t3 = await client3.BeginTransactionAsync(cancellationToken);

            // Finish t1 
            var t1Item = key1.Add("whoami", AttributeValueFactory.CreateS("t1"));

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = t1Item.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1Item, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, t1Item, true, cancellationToken);

            // Begin t2
            var t2Item = key1
                .Add("whoami", AttributeValueFactory.CreateS("t2"))
                .Add("t2stuff", AttributeValueFactory.CreateS("extra"));

            await client2.PutItemAsync(t2, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = t2Item.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t2Item, t2.Id, false, true, cancellationToken);

            // Begin and finish t3
            var t3Item = key1
                .Add("whoami", AttributeValueFactory.CreateS("t3"))
                .Add("t3stuff", AttributeValueFactory.CreateS("things"));

            await client3.PutItemAsync(t3, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = t3Item.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService3.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t3Item, t3.Id, false, true, cancellationToken);

            await client3.CommitTransactionAsync(t3, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, t3Item, true, cancellationToken);

            // Ensure t2 rolled back
            try
            {
                await client2.CommitTransactionAsync(t2, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException) { }

            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
            await transactionStore3.TryRemoveAsync(t3, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, t3Item, true, cancellationToken);
        }

        // This doesnt seem to fail when using expressions
        // 
        public async Task FailValidationInApply(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var key = testDynamoDBService.NewKey();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var t2 = await client2.BeginTransactionAsync(cancellationToken);

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET FooAttribute = :FooAttribute",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":FooAttribute", AttributeValueFactory.CreateS("Bar") }
                }
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key, true, cancellationToken);

            try
            {
                await client2.UpdateItemAsync(t2, new UpdateItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key.ToDictionary(k => k.Key, v => v.Value),
                    UpdateExpression = "SET FooAttribute = :FooAttribute",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                        {":FooAttribute", AttributeValueFactory.CreateN("1") }
                    }
                }, cancellationToken);
                Assert.Fail();
            }
            catch (AmazonServiceException e)
            {
                Assert.AreEqual("ValidationException", e.ErrorCode);
                Assert.IsTrue(e.Message.Contains("Type mismatch for attribute"));
            }

            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key, t2.Id, false, false, cancellationToken);

            try
            {
                await client2.CommitTransactionAsync(t2, cancellationToken);
                Assert.Fail();
            }
            catch (AmazonServiceException e)
            {
                Assert.AreEqual("ValidationException", e.ErrorCode);
                Assert.IsTrue(e.Message.Contains("Type mismatch for attribute"));
            }

            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key, t2.Id, false, false, cancellationToken);

            await client2.RollbackTransactionAsync(t2, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key, true, cancellationToken);

            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
        }

        public async Task UseCommittedTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var key1 = testDynamoDBService.NewKey();
            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            await client1.CommitTransactionAsync(t1, cancellationToken);

            var deleteRequest = new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            };

            try
            {
                await client1.DeleteItemAsync(t1, deleteRequest, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionCommittedException)
            {
            }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            var t2 = await client1.ResumeTransactionAsync(t1, cancellationToken);

            try
            {
                await client1.DeleteItemAsync(t1, deleteRequest, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionCommittedException)
            {
            }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            try
            {
                await client2.RollbackTransactionAsync(t2, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionCommittedException)
            {
            }

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
        }

        public async Task UseRolledBackTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();

            using var scope4 = serviceProvider.CreateScope();
            var client4 = scope4.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore4 = scope4.ServiceProvider.GetRequiredService<ITransactionStore>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var key1 = testDynamoDBService.NewKey();
            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            await client1.RollbackTransactionAsync(t1, cancellationToken);

            var deleteRequest = new DeleteItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            };

            try
            {
                await client1.DeleteItemAsync(t1, deleteRequest, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException) { }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            var t2 = await client1.ResumeTransactionAsync(t1, cancellationToken);

            try
            {
                await client1.DeleteItemAsync(t1, deleteRequest, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException) { }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            try
            {
                await client2.CommitTransactionAsync(t2, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException) { }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            var t3 = await client3.ResumeTransactionAsync(t1, cancellationToken);
            await client3.RollbackTransactionAsync(t3, cancellationToken);

            var t4 = await client4.ResumeTransactionAsync(t1, cancellationToken);

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);

            try
            {
                await client4.DeleteItemAsync(t4, deleteRequest, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException) { }
            catch (TransactionNotFoundException) { }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            await transactionStore3.TryRemoveAsync(t3, cancellationToken);
        }

        public async Task UseDeletedTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var t2 = await client1.ResumeTransactionAsync(t1, cancellationToken);
            await client1.CommitTransactionAsync(t1, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            try
            {
                await client2.CommitTransactionAsync(t2, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionNotFoundException) { }

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
        }

        public async Task DriveCommit(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();
            var versionedItemStore1 = scope1.ServiceProvider.GetRequiredService<IVersionedItemStore>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents2 = scope2.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var key2 = testDynamoDBService.NewKey();
            var item = key1.Add("attr", AttributeValueFactory.CreateS("original"));

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false, cancellationToken);

            var t2 = await client2.BeginTransactionAsync(cancellationToken);

            item = item.Add("attr2", AttributeValueFactory.CreateS("new"));
            await client2.PutItemAsync(t2, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item, t2.Id, false, true, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, key2, t2.Id, true, false, cancellationToken);

            var commitFailingTransaction = await client2.ResumeTransactionAsync(t2, cancellationToken);
            transactionServiceEvents2.OnReleaseLocksAsync = async (TransactionId t, bool isRollback, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == commitFailingTransaction.Id)
                {
                    throw new FailedYourRequestException();
                }
            };

            try
            {
                await client2.CommitTransactionAsync(commitFailingTransaction, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            transactionServiceEvents2.OnReleaseLocksAsync = null;

            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item, t2.Id, false, true, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, key2, t2.Id, true, false, cancellationToken);

            await client2.CommitTransactionAsync(t2, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false, cancellationToken);

            await client2.CommitTransactionAsync(commitFailingTransaction, cancellationToken);

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
        }

        public async Task DriveRollback(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents2 = scope2.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));

            var key2 = testDynamoDBService.NewKey();
            var item2 = key2.Add("attr2", AttributeValueFactory.CreateS("original2"));

            var key3 = testDynamoDBService.NewKey();

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item2.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, item2, true, cancellationToken);

            var t2 = await client2.BeginTransactionAsync(cancellationToken);

            var item1a = item1
                .SetItem("attr1", AttributeValueFactory.CreateS("new1"))
                .Add("attr2", AttributeValueFactory.CreateS("new1"));

            await client2.PutItemAsync(t2, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1a.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key3.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t2.Id, false, true, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, item2, t2.Id, false, false, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key3, key3, t2.Id, true, false, cancellationToken);

            var rollbackFailingTransaction = await client2.ResumeTransactionAsync(t2, cancellationToken);
            var thrown = false;
            transactionServiceEvents2.OnReleaseLocksAsync = async (TransactionId t, bool isRollback, CancellationToken c) =>
           {
               await Task.CompletedTask;
               if (t.Id == rollbackFailingTransaction.Id && !thrown && isRollback)
               {
                   thrown = true;
                   throw new FailedYourRequestException();
               }
           };

            try
            {
                await client2.RollbackTransactionAsync(rollbackFailingTransaction, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t2.Id, false, true, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, item2, t2.Id, false, false, cancellationToken);
            await assertService2.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key3, key3, t2.Id, true, false, cancellationToken);

            await client2.RollbackTransactionAsync(t2, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, item2, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key3, false, cancellationToken);

            await client2.RollbackTransactionAsync(rollbackFailingTransaction, cancellationToken);

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
        }

        public async Task RollbackCompletedTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var key1 = testDynamoDBService.NewKey();
            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, key1, t1.Id, true, true, cancellationToken);
            await client1.RollbackTransactionAsync(t1, cancellationToken);

            transactionServiceEvents1.OnDoRollbackBeginAsync = async (TransactionId t, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t == t1)
                {
                    throw new FailedYourRequestException();
                }
            };

            await client1.RollbackTransactionAsync(t1, cancellationToken);
        }

        public async Task CommitCompletedTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents2 = scope2.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var commitFailingTransaction = await client2.ResumeTransactionAsync(t1, cancellationToken);
            var key1 = testDynamoDBService.NewKey();
            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = key1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, key1, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            transactionServiceEvents2.OnDoCommitBeginAsync = async (TransactionId t, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t == commitFailingTransaction)
                {
                    throw new FailedYourRequestException();
                }

                return false;
            };

            await client2.CommitTransactionAsync(commitFailingTransaction, cancellationToken);
        }

        public async Task ResumePendingTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents2 = scope2.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));

            var key2 = testDynamoDBService.NewKey();

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);

            var t2 = await client1.ResumeTransactionAsync(t1, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, t1.Id, true, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            await client2.CommitTransactionAsync(t2, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false, cancellationToken);

            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, null, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
            await assertService2.AssertTransactionDeletedAsync(t2, cancellationToken);
        }

        public async Task ResumeAndCommitAfterTransientApplyFailure(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents2 = scope2.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnApplyRequestAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown)
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));

            var key2 = testDynamoDBService.NewKey();

            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item1.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, key1, t1.Id, true, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);

            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, t1.Id, true, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            var t3 = await client3.ResumeTransactionAsync(t1, cancellationToken);
            await client3.CommitTransactionAsync(t3, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false, cancellationToken);

            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, null, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            await client3.CommitTransactionAsync(t3, cancellationToken);

            await transactionStore3.TryRemoveAsync(t3, cancellationToken);
            await assertService2.AssertTransactionDeletedAsync(t2, cancellationToken);
        }

        public async Task ApplyOnlyOnce(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnUpdateFullyAppliedRequestsBeginAsync = async (TransactionVersion t, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown)
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateN("1"));

            var key2 = testDynamoDBService.NewKey();

            var update = new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET attr1 = :attr1",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":attr1", AttributeValueFactory.CreateN("1")}
                },
            };

            try
            {
                await client1.UpdateItemAsync(t1, update, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);

            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, t1.Id, true, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            await client2.CommitTransactionAsync(t2, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false, cancellationToken);

            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, null, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
            await assertService2.AssertTransactionDeletedAsync(t2, cancellationToken);
        }

        public async Task ResumeRollbackAfterTransientApplyFailure(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnApplyRequestAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown)
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));

            var key2 = testDynamoDBService.NewKey();

            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item1.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, key1, t1.Id, true, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);

            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);

            await client2.GetItemAsync(t2, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key2.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, key1, false, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, t1.Id, true, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            var t3 = await client3.ResumeTransactionAsync(t1, cancellationToken);
            await client3.RollbackTransactionAsync(t3, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false, cancellationToken);

            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key1, null, false, cancellationToken);
            await assertService1.AssertOldItemImageAsync(t1.Id, testDynamoDBService.GetTableName(), key2, null, false, cancellationToken);

            await transactionStore3.TryRemoveAsync(t3, cancellationToken);
            await assertService2.AssertTransactionDeletedAsync(t2, cancellationToken);
        }

        public async Task UnlockInRollbackIfNoItemImageSaved(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope0 = serviceProvider.CreateScope();
            var transactionService0 = scope0.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore0 = scope0.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService0 = scope0.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope0.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var (key0, item0) = await SetupAsync(transactionService0, assertService0, testDynamoDBService, cancellationToken);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnBackupItemImagesAsync = async (TransactionId t, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown)
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            // Change the existing item key0, failing when trying to save away the item image
            var item0a = item0.Add("attr1", AttributeValueFactory.CreateS("original1"));

            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item0a.ToDictionary(k => k.Key, v => v.Value)
                }, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key0, item0, t1.Id, false, false, cancellationToken);

            // Roll back, and ensure the item was reverted correctly
            await client1.RollbackTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item0, true, cancellationToken);
        }

        public async Task ShouldNotApplyAfterRollback(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService3 = scope3.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var barrier = new SemaphoreSlim(0);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            transactionServiceEvents1.OnAcquireLockAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                await barrier.WaitAsync(cancellationToken);
            };

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));

            var caughtRolledBackException = new SemaphoreSlim(0);

            var putTask = Task.Run(async () =>
            {
                try
                {
                    await client1.PutItemAsync(t1, new PutItemRequest
                    {
                        TableName = testDynamoDBService.GetTableName(),
                        Item = item1.ToDictionary(k => k.Key, v => v.Value)
                    }, cancellationToken);
                    Assert.Fail();
                }
                catch (TransactionRolledBackException)
                {
                    caughtRolledBackException.Release();
                }
            }, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);
            await client2.RollbackTransactionAsync(t2, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            barrier.Release(100);

            await putTask;

            Assert.AreEqual(1, caughtRolledBackException.CurrentCount);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);

            // Now start a new transaction involving key1 and make sure it will complete
            var item1a = key1.Add("attr1", AttributeValueFactory.CreateS("new"));

            var t3 = await client3.BeginTransactionAsync(cancellationToken);
            await client3.PutItemAsync(t3, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1a.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService3.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t3.Id, true, true, cancellationToken);
            await client3.CommitTransactionAsync(t3, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, true, cancellationToken);
        }

        public async Task ShouldNotApplyAfterRollbackAndDeleted(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService3 = scope3.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            // Very similar to "shouldNotApplyAfterRollback" except the transaction is rolled back and then deleted.
            var lockItemBarrier = new SemaphoreSlim(0);
            var removeTransactionBarrier = new SemaphoreSlim(0);
            var caughtFailedYourRequestExceptionBarrier = new SemaphoreSlim(0);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            transactionServiceEvents1.OnAcquireLockAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                removeTransactionBarrier.Release(1);
                await lockItemBarrier.WaitAsync(cancellationToken);
            };

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));

            var putTask = Task.Run(async () =>
            {
                try
                {
                    await client1.PutItemAsync(t1, new PutItemRequest
                    {
                        TableName = testDynamoDBService.GetTableName(),
                        Item = item1.ToDictionary(k => k.Key, v => v.Value)
                    }, cancellationToken);
                }
                catch (TransactionNotFoundException)
                {
                    caughtFailedYourRequestExceptionBarrier.Release();
                }
            }, cancellationToken);

            await removeTransactionBarrier.WaitAsync(cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);
            await client2.RollbackTransactionAsync(t2, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            await transactionStore2.TryRemoveAsync(t2, cancellationToken); // This is the key difference with shouldNotApplyAfterRollback

            lockItemBarrier.Release(100);
            await putTask;

            Assert.AreEqual(1, caughtFailedYourRequestExceptionBarrier.CurrentCount);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            // Now start a new transaction involving key1 and make sure it will complete
            var item1a = key1.Add("attr1", AttributeValueFactory.CreateS("new"));

            var t3 = await client3.BeginTransactionAsync(cancellationToken);
            await client3.PutItemAsync(t3, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1a.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);
            await assertService3.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t3.Id, true, true, cancellationToken);
            await client3.CommitTransactionAsync(t3, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, true, cancellationToken);
        }

        public async Task ShouldNotApplyAfterRollbackAndDeletedAndLeftLocked(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            using var scope3 = serviceProvider.CreateScope();
            var client3 = scope3.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore3 = scope3.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService3 = scope3.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            // Very similar to "shouldNotApplyAfterRollbackAndDeleted" except the lock is broken by a new transaction, not t1
            var lockItemBarrier = new SemaphoreSlim(0);
            var removeTransactionBarrier = new SemaphoreSlim(0);
            var caughtFailedYourRequestExceptionBarrier = new SemaphoreSlim(0);

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            transactionServiceEvents1.OnAcquireLockAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                if (t.Id == t1.Id)
                {
                    removeTransactionBarrier.Release(1);
                    await lockItemBarrier.WaitAsync(cancellationToken);
                }
            };

            var thrown = false;
            transactionServiceEvents1.OnReleaseLockFromOtherTransactionAsync = async (TransactionId t, TransactionId owningT, CancellationToken c) =>
             {
                 await Task.CompletedTask;
                 if (t.Id == t1.Id && !thrown)
                 {
                     thrown = true;
                     throw new FailedYourRequestException();
                 }
             };

            var key1 = testDynamoDBService.NewKey();
            var item1 = key1.Add("attr1", AttributeValueFactory.CreateS("original1"));
            var putTask = Task.Run(async () =>
            {
                try
                {
                    await client1.PutItemAsync(t1, new PutItemRequest
                    {
                        TableName = testDynamoDBService.GetTableName(),
                        Item = item1.ToDictionary(k => k.Key, v => v.Value)
                    }, cancellationToken);
                }
                catch (FailedYourRequestException)
                {
                    caughtFailedYourRequestExceptionBarrier.Release();
                }
            }, cancellationToken);

            await removeTransactionBarrier.WaitAsync(cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);
            await client2.RollbackTransactionAsync(t2, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            await transactionStore2.TryRemoveAsync(t2, cancellationToken);

            lockItemBarrier.Release(100);
            await putTask;

            Assert.AreEqual(1, caughtFailedYourRequestExceptionBarrier.CurrentCount);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, null, t1.Id, true, false, false, cancellationToken); // locked and "null", but don't check the transaction item

            // Now start a new transaction involving key1 and make sure it will complete
            var item1a = key1.Add("attr1", AttributeValueFactory.CreateS("new"));

            var t3 = await client3.BeginTransactionAsync(cancellationToken);
            await client3.PutItemAsync(t3, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item1a.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            await assertService3.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t3.Id, true, true, cancellationToken);
            await client3.CommitTransactionAsync(t3, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, true, cancellationToken);
        }

        public async Task RollbackAfterReadLockUpgradeAttempt(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            // After getting a read lock, attempt to write an update to the item. 
            // This will succeed in apply to the item, but will fail when trying to update the transaction item.
            // Scenario:
            // p1                    t1        i1           t2                 p2
            // ---- insert ---------->
            // --add get i1  -------->
            // --read lock+transient----------->
            //                                               <---------insert---
            //                                               <-----add get i1---
            //                                 <--------------------read i1 ----  (conflict detected)
            //                       <------------------------------read t1 ----  (going to roll it back)
            // -- add update i1 ----->
            // ---update i1  ------------------>
            //                       <------------------------------rollback t1-
            //
            //      Everything so far is fine, but this sets the stage for where the bug was
            //
            //                                X <-------------release read lock-
            //      This is where the bug used to be. p2 assumed t1 had a read lock
            //      on i1 and tried to do an optimized unlock, resulting in i1
            //      being stuck with a lock until manual lock busting.
            //      The correct behavior is for p2 not to assume that t1 has a read
            //      lock and always follow the right rollback procedures.
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents2 = scope2.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var shouldThrowAfterApply = false;

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnUpdateFullyAppliedRequestsBeginAsync = async (TransactionVersion t, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && shouldThrowAfterApply && !thrown)
                {
                    thrown = true;
                    throw new InvalidOperationException();
                }
            };

            var key1 = testDynamoDBService.NewKey();
            var key2 = testDynamoDBService.NewKey();

            // Read an item that doesn't exist to get its read lock
            var item1Returned = await client1.GetItemAsync(t1, new GetItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                ConsistentRead = true
            }, cancellationToken);
            Assert.IsTrue(!item1Returned.IsItemSet);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, false, cancellationToken);

            // Now start another transaction that is going to try to read that same item,
            // but stop after you read the competing transaction record (don't try to roll it back yet)

            // t2 waits on this for the main thread to signal it.
            var waitAfterResumeTransaction = new SemaphoreSlim(0);

            // the main thread waits on this for t2 to signal that it's ready
            var resumedTransaction = new SemaphoreSlim(0);

            // the main thread waits on this for t2 to finish with its rollback of t1
            var rolledBackT1 = new SemaphoreSlim(0);

            transactionServiceEvents2.OnResumeTransactionFinishAsync = async (TransactionId t, CancellationToken c) =>
            {
                // Signal to the main thread that t2 has loaded the transaction record.
                resumedTransaction.Release();

                try
                {
                    // Wait for the main thread to upgrade key1 to a write lock (but we won't know about it)
                    await waitAfterResumeTransaction.WaitAsync(c);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failure", ex);
                }
            };

            var getTask = Task.Run(async () =>
            {
                var t2 = await client2.BeginTransactionAsync(cancellationToken);
                // This will stop pause on waitAfterResumeTransaction once it finds that key1 is already locked by t1. 
                var item1Returned = await client2.GetItemAsync(t2, new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key1.ToDictionary(k => k.Key, v => v.Value),
                    ConsistentRead = true,
                }, cancellationToken);

                Assert.IsTrue(!item1Returned.IsItemSet);
                rolledBackT1.Release();
            }, cancellationToken);

            // Wait for t2 to get to the point where it loaded the t1 transaction record.
            await resumedTransaction.WaitAsync(cancellationToken);

            // Now change that getItem to an updateItem in t1
            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
                UpdateExpression = "SET asdf = :asdf",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":asdf", AttributeValueFactory.CreateS("wef")}
                },
            }, cancellationToken);

            // Now let t2 continue on and roll back t1
            waitAfterResumeTransaction.Release();

            // Wait for t2 to finish rolling back t1
            await rolledBackT1.WaitAsync(cancellationToken);

            // T1 should be rolled back now and unable to do stuff
            try
            {
                await client1.GetItemAsync(t1, new GetItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Key = key2.ToDictionary(k => k.Key, v => v.Value),
                    ConsistentRead = true
                }, cancellationToken);
                Assert.Fail();
            }
            catch (TransactionRolledBackException)
            {
                // expected
            }
        }

        // Same as shouldNotLockAndApplyAfterRollbackAndDeleted except make t3 do the unlock, not t1.

        public async Task BasicNewItemRollback(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, true, cancellationToken);

            await client1.RollbackTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);

            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
        }

        public async Task BasicNewItemCommit(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();

            await client1.UpdateItemAsync(t1, new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = key1.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);
            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, key1, true, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
        }

        public async Task MissingTableName(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key1 = testDynamoDBService.NewKey();

            try
            {
                await client1.UpdateItemAsync(t1, new UpdateItemRequest
                {
                    Key = key1.ToDictionary(k => k.Key, v => v.Value)
                }, cancellationToken);
                Assert.Fail();
            }
            catch (InvalidOperationException e)
            {
                Assert.IsTrue(e.Message.Contains("TableName must not be null"));
            }
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            await client1.RollbackTransactionAsync(t1, cancellationToken);
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, false, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
        }

        public async Task EmptyTransaction(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            await client1.CommitTransactionAsync(t1, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
            await assertService1.AssertTransactionDeletedAsync(t1, cancellationToken);
        }

        public async Task MissingKey(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            try
            {
                await client1.UpdateItemAsync(t1, new UpdateItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                }, cancellationToken);
                Assert.Fail();
            }
            catch (InvalidOperationException e)
            {
                Assert.IsTrue(e.Message.Contains("The request key cannot be empty"));
            }
            await client1.RollbackTransactionAsync(t1, cancellationToken);
            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
        }

        /**
        * This test makes a transaction with two large items, each of which are just below
        * the DynamoDB item size limit (currently 400 KB).
*/
        // 
        // public async Task TooMuchDataInTransaction()
        // {
        //     
        //     
        //     
        //     
        //     using var scope1 = serviceProvider.CreateScope();
        //     var client1 = scope1.ServiceProvider.GetRequiredService<ITransactionService>();
        //     var transactionStore = serviceProvider.GetRequiredService<ITransactionItemService>();
        //     var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

        //     var t1 = await client1.BeginTransactionAsync(cancellationToken);
        //     var t2 = await client2.BeginTransactionAsync(cancellationToken);
        //     var key1 = testDynamoDBService.NewKey();
        //     var key2 = testDynamoDBService.NewKey();

        //     // Write item 1 as a starting point
        //     StringBuilder sb = new StringBuilder();
        //     for (int i = 0; i < (MAX_ITEM_SIZE_BYTES / 1.5); i++)
        //     {
        //         sb.append("a");
        //     }
        //     String bigString = sb.toString();

        //     var item1 = key1
        //     .Add("bigattr", AttributeValueFactory.CreateS("little"));
        //     await client1.PutItemAsync(t1, new PutItemRequest
        //     {
        //         TableName = testDynamoDBService.GetTableName(),
        //         Item = item1.ToDictionary(k => k.Key, v => v.Value));

        //     await AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1, t1.Id, true, true, transactionStore, dynamoDB, cancellationToken);

        //     await client1.CommitTransactionAsync(t1, cancellationToken);

        //     await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1, true, cancellationToken);

        //     var item1a = new HashMap<String, AttributeValue>(key1);
        //     item1a.put("bigattr", AttributeValueFactory.CreateS(bigString));

        //     await client2.PutItemAsync(t2, new PutItemRequest
        //     {
        //         TableName = testDynamoDBService.GetTableName(),
        //         Item = item1a));

        //     await AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t2.Id, false, true);

        //     var item2 = new HashMap<String, AttributeValue>(key2);
        //     item2.put("bigattr", AttributeValueFactory.CreateS(bigString));

        //     try
        //     {
        //         await client2.PutItemAsync(t2, new PutItemRequest
        //         {
        //             TableName = testDynamoDBService.GetTableName(),
        //             Item = item2));
        //         Assert.Fail();
        //     }
        //     catch (InvalidRequestException e) { }

        //     await AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, false);
        //     await AssertItemLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, t2.Id, false, true);

        //     item2.put("bigattr", AttributeValueFactory.CreateS("fitsThisTime"));
        //     await client2.PutItemAsync(t2, new PutItemRequest
        //     {
        //         TableName = testDynamoDBService.GetTableName(),
        //         Item = item2));

        //     await AssertItemLockedAsync(testDynamoDBService.GetTableName(), key2, item2, t2.Id, true, true);

        //     await client2.CommitTransactionAsync(t2, cancellationToken);

        //     await AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key1, item1a, true);
        //     await AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key2, item2, true);

        //     await transactionService.DeleteTransactionAsync(t1.Id, cancellationToken);
        //     await transactionService.DeleteTransactionAsync(t2.Id, cancellationToken);
        // }

        public async Task ContainsBinaryAttributes(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key = testDynamoDBService.NewKey();
            var item = key
                .Add("attr_b", AttributeValueFactory.CreateB(new MemoryStream(Encoding.UTF8.GetBytes("asdf\n\t\u0123"))))
                .Add("attr_bs", AttributeValueFactory.CreateBS([
                        new(Encoding.UTF8.GetBytes("asdf\n\t\u0123")),
                        new(Encoding.UTF8.GetBytes("wef"))
                    ]
                ));

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key, item, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key, item, true, cancellationToken);
        }

        public async Task ContainsJSONAttributes(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key = testDynamoDBService.NewKey();
            var item = key
                .Add("attr_json", AttributeValueFactory.CreateM(GetJsonAttributeMap()));

            await client1.PutItemAsync(t1, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item.ToDictionary(k => k.Key, v => v.Value),
            }, cancellationToken);

            await assertService1.AssertItemLockedAsync(testDynamoDBService.GetTableName(), key, item, t1.Id, true, true, cancellationToken);

            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key, item, true, cancellationToken);
        }

        private static Dictionary<string, AttributeValue> GetJsonAttributeMap()
        {
            return new Dictionary<string, AttributeValue>
            {
                { "attr_s", AttributeValueFactory.CreateS("s")},
                { "attr_n", AttributeValueFactory.CreateN("1") },
                { "attr_b", AttributeValueFactory.CreateB(new MemoryStream(Encoding.UTF8.GetBytes("asdf")))},
                { "attr_ss", AttributeValueFactory.CreateSS(["a", "b"])},
                // { "attr_ns", AttributeValueFactory.CreateNS(new List<string> { "1", "2" })}, // TODO Insertion results in reverse order ????
                {
                    "attr_bs",AttributeValueFactory.CreateBS(
                        [
                            new(Encoding.UTF8.GetBytes("asdf")),
                            new(Encoding.UTF8.GetBytes("ghjk"))
                        ]
                    )
                },
                { "attr_bool", AttributeValueFactory.CreateBOOL(true)},
                {
                    "attr_l",
                    AttributeValueFactory.CreateL(
                        [
                            AttributeValueFactory.CreateS("s"),
                            AttributeValueFactory.CreateN("1"),
                            AttributeValueFactory.CreateB(new MemoryStream(Encoding.UTF8.GetBytes("asdf"))),
                            AttributeValueFactory.CreateBOOL(true),
                            AttributeValueFactory.CreateNULL(true)
                        ]
                    )
                },
                { "attr_null", AttributeValueFactory.CreateNULL(true)}
            };
        }

        public async Task ContainsSpecialAttributes(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);
            var key = testDynamoDBService.NewKey();
            var item = key.Add(ItemAttributeName.TXID.Value, AttributeValueFactory.CreateS("asdf"));

            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
                Assert.Fail();
            }
            catch (InvalidOperationException e)
            {
                Assert.IsTrue(e.Message.Contains("Request must not contain a reserved attribute"));
            }

            item = item
                .Add(ItemAttributeName.TRANSIENT.Value, AttributeValueFactory.CreateS("asdf"))
                .Remove(ItemAttributeName.TXID.Value);

            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
                Assert.Fail();
            }
            catch (InvalidOperationException e)
            {
                Assert.IsTrue(e.Message.Contains("Request must not contain a reserved attribute"));
            }

            item = item
                .Add(ItemAttributeName.APPLIED.Value, AttributeValueFactory.CreateS("asdf"))
                .Remove(ItemAttributeName.TRANSIENT.Value);

            try
            {
                await client1.PutItemAsync(t1, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = item.ToDictionary(k => k.Key, v => v.Value),
                }, cancellationToken);
                Assert.Fail();
            }
            catch (InvalidOperationException e)
            {
                Assert.IsTrue(e.Message.Contains("Request must not contain a reserved attribute"));
            }
        }

#pragma warning disable IDE0060 // Remove unused parameter
        public async Task ItemTooLargeToLock(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
        }

        public async Task ItemTooLargeToApply(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
        }

        public async Task ItemTooLargeToSavePreviousVersion(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
        }
#pragma warning restore IDE0060 // Remove unused parameter

        public async Task Failover(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            using var scope2 = serviceProvider.CreateScope();
            var client2 = scope2.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore2 = scope2.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService2 = scope2.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var t1 = await client1.BeginTransactionAsync(cancellationToken);

            var thrown = false;
            transactionServiceEvents1.OnAcquireLockAsync = async (TransactionId t, AmazonDynamoDBRequest r, CancellationToken c) =>
            {
                await Task.CompletedTask;
                if (t.Id == t1.Id && !thrown)
                {
                    thrown = true;
                    throw new FailedYourRequestException();
                }
            };

            // prepare a request
            var callerRequestKey = testDynamoDBService.NewKey();
            var callerRequest = new UpdateItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Key = callerRequestKey.ToDictionary(k => k.Key, v => v.Value)
            };

            try
            {
                await client1.UpdateItemAsync(t1, callerRequest, cancellationToken);
                Assert.Fail();
            }
            catch (FailedYourRequestException) { }
            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), callerRequestKey, false, cancellationToken);

            // The non-failing manager
            var t2 = await client2.ResumeTransactionAsync(t1, cancellationToken);
            await client2.CommitTransactionAsync(t2, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), callerRequestKey, true, cancellationToken);

            // If this attempted to apply again, this would fail because of the failing client
            await client1.CommitTransactionAsync(t1, cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), callerRequestKey, true, cancellationToken);

            await transactionStore1.TryRemoveAsync(t1, cancellationToken);
            await transactionStore2.TryRemoveAsync(t2, cancellationToken);
        }

        public async Task OneTransactionPerItem(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var transactionStore1 = scope1.ServiceProvider.GetRequiredService<ITransactionStore>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();
            var transactionServiceEvents1 = scope1.ServiceProvider.GetRequiredService<ITransactionServiceEvents>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var transaction = await client1.BeginTransactionAsync(cancellationToken);
            var key = testDynamoDBService.NewKey();

            await client1.PutItemAsync(transaction, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = key.ToDictionary(k => k.Key, v => v.Value)
            }, cancellationToken);

            try
            {
                await client1.PutItemAsync(transaction, new PutItemRequest
                {
                    TableName = testDynamoDBService.GetTableName(),
                    Item = key.ToDictionary(k => k.Key, v => v.Value)
                }, cancellationToken);
                Assert.Fail();
            }
            catch (DuplicateRequestException)
            {
                await client1.RollbackTransactionAsync(transaction, cancellationToken);
            }

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key, false, cancellationToken);
            await transactionStore1.TryRemoveAsync(transaction, cancellationToken);
        }

        public async Task CanUseTransactWriteItemsWithGreaterThan25ItemsAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var keys = Enumerable
                .Range(0, 26)
                .Select(i => testDynamoDBService.NewKey())
                .ToImmutableList();

            var transactWriteItems = keys
                .Select(key => new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = testDynamoDBService.GetTableName(),
                        Item = key.ToDictionary(k => k.Key, v => v.Value)
                    }
                })
                .Concat([ new TransactWriteItem{
                    ConditionCheck = new ConditionCheck{
                        TableName = testDynamoDBService.GetTableName(),
                        Key = testDynamoDBService.NewKey().ToDictionary(k => k.Key, v => v.Value),
                        ConditionExpression = $"attribute_not_exists({nameof(RDFTriple.Subject)})",
                    }
                } ])
                .ToList();

            await client1.TransactWriteItemsAsync(
                new TransactWriteItemsRequest { TransactItems = transactWriteItems },
                cancellationToken);

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), keys.First(), keys.First(), true, cancellationToken);
        }

        public async Task CanRollbackUseTransactWriteItemsWithGreaterThan25ItemsAsync(ServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            using var scope1 = serviceProvider.CreateScope();
            var client1 = scope1.ServiceProvider.GetRequiredService<IAmazonDynamoDBWithTransactions>();
            var assertService1 = scope1.ServiceProvider.GetRequiredService<TestAssertService>();

            var testDynamoDBService = scope1.ServiceProvider.GetRequiredService<ITestDynamoDBService>();

            var keys = Enumerable
                .Range(0, 26)
                .Select(i => testDynamoDBService.NewKey())
                .ToImmutableList();

            var transactWriteItems = keys
                .Select(key => new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = testDynamoDBService.GetTableName(),
                        Item = key.ToDictionary(k => k.Key, v => v.Value)
                    }
                })
                .Concat([ new TransactWriteItem{
                    ConditionCheck = new ConditionCheck{
                        TableName = testDynamoDBService.GetTableName(),
                        Key = testDynamoDBService.NewKey().ToDictionary(k => k.Key, v => v.Value),
                        ConditionExpression = $"attribute_exists({nameof(RDFTriple.Subject)})",
                    }
                } ])
                .ToList();

            await Assert.ThrowsExceptionAsync<TransactionCanceledException>(async () => await client1.TransactWriteItemsAsync(
                new TransactWriteItemsRequest { TransactItems = transactWriteItems }, cancellationToken));

            await assertService1.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), keys.First(), null, false, cancellationToken);
        }

        private static async Task<SetupData> SetupAsync(
            IAmazonDynamoDBWithTransactions transactionService,
            TestAssertService assertService,
            ITestDynamoDBService testDynamoDBService,
            CancellationToken cancellationToken)
        {
            // dynamodb.reset();
            var t = await transactionService.BeginTransactionAsync(cancellationToken);
            var key0 = testDynamoDBService.NewKey();
            var item0 = ImmutableDictionary<string, AttributeValue>
                .Empty
                .AddRange(key0)
                .Add("s_someattr", AttributeValueFactory.CreateS("val"))
                .Add("ss_otherattr", AttributeValueFactory.CreateSS(["one", "two"]));

            var putResponse = await transactionService.PutItemAsync(t, new PutItemRequest
            {
                TableName = testDynamoDBService.GetTableName(),
                Item = item0.ToDictionary(k => k.Key, v => v.Value),
                ReturnValues = ReturnValue.ALL_OLD
            }, cancellationToken);

            Assert.IsTrue(putResponse.Attributes.Count == 0);
            await transactionService.CommitTransactionAsync(t, cancellationToken);
            await assertService.AssertItemNotLockedAsync(testDynamoDBService.GetTableName(), key0, item0, true, cancellationToken);
            return new SetupData(key0, item0);
        }

        // private static async Task DeleteAllFromTableAsync(
        //     string tableName, IAmazonDynamoDB dynamoDB, IAmazonDynamoDBKeyService dynamoDBKeyService, CancellationToken cancellationToken)
        // {
        //     while (true)
        //     {
        //         cancellationToken.ThrowIfCancellationRequested();

        //         var scanRequest = new ScanRequest { TableName = tableName, Limit = 25 };
        //         var scanResponse = await dynamoDB.ScanAsync(scanRequest, cancellationToken);
        //         if (scanResponse.Items.Count == 0)
        //         {
        //             return;
        //         }

        //         var writeRequests = await Task.WhenAll(
        //             scanResponse
        //                 .Items
        //                 .Select(async item => new WriteRequest
        //                 {
        //                     DeleteRequest = new DeleteRequest
        //                     {
        //                         Key = (await dynamoDBKeyService.CreateKeyMapAsync(scanRequest.TableName, item.ToImmutableDictionary(), cancellationToken)).ToDictionary(k => k.Key, v => v.Value)
        //                     }
        //                 }));

        //         await dynamoDB.BatchWriteItemAsync(new BatchWriteItemRequest
        //         {
        //             RequestItems = new Dictionary<string, List<WriteRequest>>{
        //                 { scanRequest.TableName, writeRequests.ToList()}
        //             }
        //         }, cancellationToken);
        //     }
        // }
    }
}