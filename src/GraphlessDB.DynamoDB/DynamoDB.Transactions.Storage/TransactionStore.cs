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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Options;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class DefaultTransactionStore(
        IOptionsSnapshot<TransactionStoreOptions> options,
        IAmazonDynamoDB client,
        IRequestRecordSerializer requestSerializerService) : ITransactionStore
    {
        public const string StatePending = "P";
        public const string StateCommitted = "C";
        public const string StateRolledBack = "R";

        private readonly Lock _locker = new();
        private ImmutableDictionary<TransactionId, Transaction> _transactionsById = ImmutableDictionary<TransactionId, Transaction>.Empty;

        public async Task<ImmutableList<Transaction>> ListAsync(int limit, CancellationToken cancellationToken)
        {
            var request = new ScanRequest
            {
                TableName = options.Value.TransactionTableName,
                AttributesToGet = [TransactionAttributeName.TXID.Value],
                ConsistentRead = true,
                Limit = limit,
            };

            var response = await client.ScanAsync(request, cancellationToken);
            return response
                .Items
                .Select(GetTransaction)
                .ToImmutableList();
        }

        public Dictionary<string, AttributeValue> GetKey(TransactionId id)
        {
            return new Dictionary<string, AttributeValue> {
                { TransactionAttributeName.TXID.Value, AttributeValueFactory.CreateS(id.Id) }
            };
        }

        public async Task<bool> ContainsAsync(
            TransactionId id,
            CancellationToken cancellationToken)
        {
            var getItemRequest = new GetItemRequest
            {
                TableName = options.Value.TransactionTableName,
                Key = GetKey(id),
                AttributesToGet = [TransactionAttributeName.TXID.Value],
                ConsistentRead = true,
            };

            var getItemResponse = await client.GetItemAsync(getItemRequest, cancellationToken);

            return getItemResponse.IsItemSet;
        }

        public async Task AddAsync(
            Transaction transaction,
            CancellationToken cancellationToken)
        {
            try
            {
                var putItemRequest = new PutItemRequest
                {
                    TableName = options.Value.TransactionTableName,
                    ConditionExpression = $"attribute_not_exists(#{TransactionAttributeName.TXID.Value})",
                    Item = GetItem(transaction),
                    ExpressionAttributeNames = new Dictionary<string, string> {
                        {$"#{TransactionAttributeName.TXID.Value}", TransactionAttributeName.TXID.Value},
                    },
                    ReturnValues = ReturnValue.NONE,
                };

                await client.PutItemAsync(putItemRequest, cancellationToken);

                lock (_locker)
                {
                    _transactionsById = _transactionsById.Add(transaction.GetId(), transaction);
                }
            }
            catch (ConditionalCheckFailedException ex)
            {
                transaction = await GetAsync(transaction.GetId(), true, cancellationToken);
                throw transaction.State switch
                {
                    TransactionState.Committing => new TransactionCommittedException(transaction.Id, "Transaction is already committing", ex),
                    TransactionState.Committed => new TransactionCommittedException(transaction.Id, "Transaction is already committed", ex),
                    TransactionState.RollingBack => new TransactionRolledBackException(transaction.Id, "Transaction is already rolling back", ex),
                    TransactionState.RolledBack => new TransactionRolledBackException(transaction.Id, "Transaction is already rolled back", ex),
                    _ => new TransactionException(transaction.Id, "Failed to add transaction", ex),
                };
            }
        }

        public async Task<Transaction> GetAsync(
            TransactionId id,
            bool forceFetch,
            CancellationToken cancellationToken)
        {
            if (!forceFetch && _transactionsById.TryGetValue(id, out var latestTransactionVersion))
            {
                return latestTransactionVersion;
            }

            var getItemRequest = new GetItemRequest
            {
                TableName = options.Value.TransactionTableName,
                Key = GetKey(id),
                ConsistentRead = true,
            };

            var getItemResponse = await client.GetItemAsync(getItemRequest, cancellationToken);
            if (!getItemResponse.IsItemSet)
            {
                throw new TransactionNotFoundException(id.Id);
            }

            var transaction = GetTransaction(getItemResponse.Item);
            lock (_locker)
            {
                _transactionsById = _transactionsById.SetItem(transaction.GetId(), transaction);
            }

            return transaction;
        }

        public async Task<Transaction> UpdateAsync(Transaction transaction, CancellationToken cancellationToken)
        {
            try
            {
                var updateItemRequest = new UpdateItemRequest
                {
                    TableName = options.Value.TransactionTableName,
                    Key = GetKey(transaction.GetId()),
                    ConditionExpression = $"#{TransactionAttributeName.VERSION.Value} = :{TransactionAttributeName.VERSION.Value}_expected",
                    UpdateExpression = $"SET #{TransactionAttributeName.VERSION.Value} = :{TransactionAttributeName.VERSION.Value}, #{TransactionAttributeName.STATE.Value} = :{TransactionAttributeName.STATE.Value}, #{TransactionAttributeName.DATE.Value} = :{TransactionAttributeName.DATE.Value}, #{TransactionAttributeName.FINALIZED.Value} = :{TransactionAttributeName.FINALIZED.Value}",
                    ExpressionAttributeNames = new Dictionary<string, string> {
                    { $"#{TransactionAttributeName.VERSION.Value}", TransactionAttributeName.VERSION.Value },
                    { $"#{TransactionAttributeName.STATE.Value}", TransactionAttributeName.STATE.Value },
                    { $"#{TransactionAttributeName.FINALIZED.Value}", TransactionAttributeName.FINALIZED.Value },
                    { $"#{TransactionAttributeName.DATE.Value}", TransactionAttributeName.DATE.Value },
                },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    { $":{TransactionAttributeName.VERSION.Value}_expected", AttributeValueFactory.CreateN((transaction.Version - 1).ToString(CultureInfo.InvariantCulture))},
                    { $":{TransactionAttributeName.VERSION.Value}", AttributeValueFactory.CreateN(transaction.Version.ToString(CultureInfo.InvariantCulture))},
                    { $":{TransactionAttributeName.STATE.Value}", AttributeValueFactory.CreateS(StateToString(transaction.State))},
                    { $":{TransactionAttributeName.FINALIZED.Value}", AttributeValueFactory.CreateS(IsCompletedToString(transaction.State))},
                    { $":{TransactionAttributeName.DATE.Value}", AttributeValueFactory.CreateN(transaction.LastUpdateDateTime.Ticks.ToString(CultureInfo.InvariantCulture))},
                },
                    ReturnValues = ReturnValue.NONE,
                };

                await client.UpdateItemAsync(updateItemRequest, cancellationToken);

                lock (_locker)
                {
                    _transactionsById = _transactionsById.SetItem(transaction.GetId(), transaction);
                }

                return transaction;
            }
            catch (ConditionalCheckFailedException ex)
            {
                transaction = await GetAsync(transaction.GetId(), true, cancellationToken);
                throw transaction.State switch
                {
                    TransactionState.Committing => new TransactionCommittedException(transaction.Id, "Transaction is already committing", ex),
                    TransactionState.Committed => new TransactionCommittedException(transaction.Id, "Transaction is already committed", ex),
                    TransactionState.RollingBack => new TransactionRolledBackException(transaction.Id, "Transaction is already rolling back", ex),
                    TransactionState.RolledBack => new TransactionRolledBackException(transaction.Id, "Transaction is already rolled back", ex),
                    _ => new TransactionException(transaction.Id, "Failed to update transaction", ex),
                };
            }
        }

        public async Task<Transaction> AppendRequestAsync(
            Transaction transaction,
            AmazonDynamoDBRequest request,
            CancellationToken cancellationToken)
        {
            try
            {
                var newVersion = transaction.Version + 1;
                var requestRecord = RequestRecord.Create(newVersion, request);
                var updatedTransaction = transaction with
                {
                    Requests = transaction.Requests.Add(requestRecord),
                    Version = newVersion,
                };

                var updateItemRequest = new UpdateItemRequest
                {
                    TableName = options.Value.TransactionTableName,
                    Key = GetKey(updatedTransaction.GetId()),
                    ConditionExpression = $"#{TransactionAttributeName.STATE.Value} = :{TransactionAttributeName.STATE.Value}_expected AND #{TransactionAttributeName.VERSION.Value} = :{TransactionAttributeName.VERSION.Value}_expected",
                    UpdateExpression = $"ADD #{TransactionAttributeName.REQUESTS.Value} :{TransactionAttributeName.REQUESTS.Value} SET #{TransactionAttributeName.VERSION.Value} = :{TransactionAttributeName.VERSION.Value}, #{TransactionAttributeName.DATE.Value} = :{TransactionAttributeName.DATE.Value}",
                    ExpressionAttributeNames = new Dictionary<string, string> {
                        { $"#{TransactionAttributeName.STATE.Value}", TransactionAttributeName.STATE.Value },
                        { $"#{TransactionAttributeName.VERSION.Value}", TransactionAttributeName.VERSION.Value },
                        { $"#{TransactionAttributeName.REQUESTS.Value}", TransactionAttributeName.REQUESTS.Value },
                        { $"#{TransactionAttributeName.DATE.Value}", TransactionAttributeName.DATE.Value },
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                        { $":{TransactionAttributeName.STATE.Value}_expected", AttributeValueFactory.CreateS(StatePending) },
                        { $":{TransactionAttributeName.VERSION.Value}_expected", AttributeValueFactory.CreateN(transaction.Version.ToString(CultureInfo.InvariantCulture))},
                        { $":{TransactionAttributeName.VERSION.Value}", AttributeValueFactory.CreateN(updatedTransaction.Version.ToString(CultureInfo.InvariantCulture))},
                        { $":{TransactionAttributeName.REQUESTS.Value}", AttributeValueFactory.CreateBS([
                            new(requestSerializerService.Serialize(requestRecord))
                        ])},
                        { $":{TransactionAttributeName.DATE.Value}", AttributeValueFactory.CreateN(DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture)) },
                    },
                    ReturnValues = ReturnValue.NONE,
                };

                await client.UpdateItemAsync(updateItemRequest, cancellationToken);

                lock (_locker)
                {
                    _transactionsById = _transactionsById.SetItem(updatedTransaction.GetId(), updatedTransaction);
                }

                return updatedTransaction;
            }
            catch (ConditionalCheckFailedException ex)
            {
                transaction = await GetAsync(transaction.GetId(), true, cancellationToken);
                throw transaction.State switch
                {
                    TransactionState.Committing => new TransactionCommittedException(transaction.Id, "Transaction is already committing", ex),
                    TransactionState.Committed => new TransactionCommittedException(transaction.Id, "Transaction is already committed", ex),
                    TransactionState.RollingBack => new TransactionRolledBackException(transaction.Id, "Transaction is already rolling back", ex),
                    TransactionState.RolledBack => new TransactionRolledBackException(transaction.Id, "Transaction is already rolled back", ex),
                    // TransactionState.Active => new UnknownCompletedTransactionException(transaction.Id, "Attempted to add a request to a transaction that was not in state " + TransactionState.Active + ", state is " + transaction.State),
                    _ => new TransactionException(transaction.Id, "Failed to add request to transaction", ex),
                };
            }
        }

        private static Dictionary<string, AttributeValue> GetItem(Transaction transaction)
        {
            return new Dictionary<string, AttributeValue> {
                {TransactionAttributeName.TXID.Value, AttributeValueFactory.CreateS(transaction.Id)},
                {TransactionAttributeName.STATE.Value, AttributeValueFactory.CreateS(StateToString(transaction.State))},
                {TransactionAttributeName.VERSION.Value, AttributeValueFactory.CreateN(transaction.Version.ToString(CultureInfo.InvariantCulture))},
                {TransactionAttributeName.FINALIZED.Value, AttributeValueFactory.CreateS(IsCompletedToString(transaction.State))},
                {TransactionAttributeName.DATE.Value, AttributeValueFactory.CreateN(transaction.LastUpdateDateTime.Ticks.ToString(CultureInfo.InvariantCulture))},
            };
        }

        public async Task RemoveAsync(
            TransactionId id,
            CancellationToken cancellationToken)
        {
            try
            {
                var request = new DeleteItemRequest
                {
                    TableName = options.Value.TransactionTableName,
                    Key = GetKey(id),
                    Expected = new Dictionary<string, ExpectedAttributeValue>{{
                        TransactionAttributeName.FINALIZED.Value,
                        new ExpectedAttributeValue {
                            Value = AttributeValueFactory.CreateS(TransactionConstants.BooleanTrueAttrVal)
                        }
                    }}
                };

                await client.DeleteItemAsync(request, cancellationToken);
            }
            catch (ConditionalCheckFailedException ex)
            {
                if (!await ContainsAsync(id, cancellationToken))
                {
                    throw new TransactionNotFoundException(id.Id);
                }

                throw new TransactionException(id.Id, "Transaction was completed but could not be deleted", ex);
            }
        }

        private static string StateToString(TransactionState state)
        {
            return state switch
            {
                TransactionState.Active => StatePending,
                TransactionState.Committing => StateCommitted,
                TransactionState.Committed => StateCommitted,
                TransactionState.RollingBack => StateRolledBack,
                TransactionState.RolledBack => StateRolledBack,
                _ => throw new NotSupportedException(),
            };
        }

        private static string IsCompletedToString(TransactionState state)
        {
            return IsCompletedToString(state is TransactionState.Committed or TransactionState.RolledBack);
        }

        private static string IsCompletedToString(bool value)
        {
            return value ? "1" : "0";
        }

        private static string GetId(Dictionary<string, AttributeValue> item)
        {
            return item[TransactionAttributeName.TXID.Value].S;
        }

        private static TransactionState GetState(Dictionary<string, AttributeValue> item)
        {
            var isCompleted = item[TransactionAttributeName.FINALIZED.Value].S == IsCompletedToString(true);
            var hasAttribute = item.TryGetValue(TransactionAttributeName.STATE.Value, out var attributeValue);
            var transactionState = hasAttribute && attributeValue != null ? attributeValue.S : null;
            if (StateCommitted == transactionState)
            {
                return isCompleted ? TransactionState.Committed : TransactionState.Committing;
            }
            else if (StateRolledBack == transactionState)
            {
                return isCompleted ? TransactionState.RolledBack : TransactionState.RollingBack;
            }
            else if (StatePending == transactionState)
            {
                return TransactionState.Active;
            }
            else
            {
                throw new InvalidOperationException("Unrecognized transaction state: " + transactionState);
            }
        }

        private static int GetVersion(Dictionary<string, AttributeValue> item)
        {
            try
            {
                return int.Parse(item[TransactionAttributeName.VERSION.Value].N, CultureInfo.InvariantCulture);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Transaction version number is missing or invalid", ex);
            }
        }

        private static DateTime GetLastUpdateDateTime(Dictionary<string, AttributeValue> item)
        {
            var hasAttribute = item.TryGetValue(TransactionAttributeName.DATE.Value, out var attributeValue);
            if (!hasAttribute || attributeValue == null || attributeValue?.N == null)
            {
                throw new InvalidOperationException("Expected date attribute to be defined");
            }

            try
            {
                var ticks = long.Parse(attributeValue.N, CultureInfo.InvariantCulture);
                return new DateTime(ticks);
            }
            catch (FormatException ex)
            {
                throw new InvalidOperationException("Excpected valid date attribute, was: " + attributeValue.N, ex);
            }
        }

        private ImmutableList<RequestRecord> GetRequests(Dictionary<string, AttributeValue> item)
        {
            var hasAttribute = item.TryGetValue(TransactionAttributeName.REQUESTS.Value, out var attributeValue);
            if (!hasAttribute || attributeValue == null)
            {
                return [];
            }

            return attributeValue
                .BS
                .Select(b => requestSerializerService.Deserialize(b.ToArray()))
                .ToImmutableList();
        }

        private Transaction GetTransaction(Dictionary<string, AttributeValue> item)
        {
            return new Transaction(
                GetId(item),
                GetState(item),
                GetVersion(item),
                GetLastUpdateDateTime(item),
                GetRequests(item));
        }
    }
}
