/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    [JsonSerializable(typeof(ImmutableList<ItemRecord>))]
    public partial class ItemImageStoreSerializerContext : JsonSerializerContext
    {
    }
}
