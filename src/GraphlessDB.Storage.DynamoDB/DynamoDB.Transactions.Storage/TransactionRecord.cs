/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    // Contains an image of the transaction item in DynamoDB, and methods to change that item.
    // If any of those attempts to change the transaction fail, the item needs to be thrown away, re-fetched,
    // and the change applied via the new item.
    // Version - Gets the version of the transaction image currently loaded.  Useful for determining if the item has changed when committing the transaction.
    public sealed record Transaction(
          string Id,
          TransactionState State,
          int Version,
          DateTime LastUpdateDateTime,
          ImmutableList<RequestRecord> Requests)
    {
        public static Transaction CreateNew()
        {
            return new Transaction(Guid.NewGuid().ToString(), TransactionState.Active, 1, DateTime.UtcNow, []);
        }

        public TransactionId GetId()
        {
            return new TransactionId(Id);
        }

        public TransactionVersion GetVersion()
        {
            return new TransactionVersion(Id, Version);
        }
    };
}