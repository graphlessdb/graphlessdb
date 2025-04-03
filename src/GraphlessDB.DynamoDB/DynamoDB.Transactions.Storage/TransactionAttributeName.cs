/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record TransactionAttributeName(string Value)
    {
        public override string ToString()
        {
            return Value;
        }

        public static readonly TransactionAttributeName TXID = new("_TxId");
        public static readonly TransactionAttributeName DATE = new("_TxD");
        public static readonly TransactionAttributeName REQUESTS = new("_TxR");
        public static readonly TransactionAttributeName STATE = new("_TxS");
        public static readonly TransactionAttributeName VERSION = new("_TxV");
        public static readonly TransactionAttributeName FINALIZED = new("_TxF");

        public static readonly ImmutableHashSet<TransactionAttributeName> Values = ImmutableHashSet<TransactionAttributeName>
            .Empty
            .Add(TXID)
            .Add(DATE)
            .Add(REQUESTS)
            .Add(STATE)
            .Add(VERSION)
            .Add(FINALIZED);
    }
}