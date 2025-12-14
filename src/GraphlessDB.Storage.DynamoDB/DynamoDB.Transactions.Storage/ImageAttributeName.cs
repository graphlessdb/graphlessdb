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
    public sealed record ImageAttributeName(string Value)
    {
        public override string ToString()
        {
            return Value;
        }

        public static readonly ImageAttributeName ImageId = new("_TxI");
        public static readonly ImageAttributeName ImageValue = new("_TxIV");

        public static readonly ImmutableHashSet<ImageAttributeName> Values = ImmutableHashSet<ImageAttributeName>
            .Empty
            .Add(ImageId)
            .Add(ImageValue);
    }
}
