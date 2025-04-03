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
    public sealed record ItemAttributeName(string Value)
    {
        public override string ToString()
        {
            return Value;
        }

        public static readonly ItemAttributeName TXID = new("_TxId");
        public static readonly ItemAttributeName DATE = new("_TxD");
        public static readonly ItemAttributeName TRANSIENT = new("_TxT");
        public static readonly ItemAttributeName APPLIED = new("_TxA");

        public static readonly ImmutableHashSet<ItemAttributeName> Values = ImmutableHashSet<ItemAttributeName>
            .Empty
            .Add(TXID)
            .Add(DATE)
            .Add(TRANSIENT)
            .Add(APPLIED);
    }
}