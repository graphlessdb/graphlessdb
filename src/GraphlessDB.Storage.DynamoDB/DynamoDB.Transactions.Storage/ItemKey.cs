/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.Collections.Immutable;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record ItemKey(string TableName, ImmutableDictionarySequence<string, ImmutableAttributeValue> Key)
    {
        public static ItemKey Create(string tableName, ImmutableDictionary<string, AttributeValue> mutableKey)
        {
            return new ItemKey(tableName,
                mutableKey.ToImmutableDictionary(
                    k => k.Key,
                    v => ImmutableAttributeValue.Create(v.Value)).ToImmutableDictionarySequence());
        }
    }
}