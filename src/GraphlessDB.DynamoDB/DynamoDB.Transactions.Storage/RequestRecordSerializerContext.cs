/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Text.Json;
using System.Text.Json.Serialization;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    [JsonSerializable(typeof(RequestRecord))]
    public partial class RequestRecordSerializerContext : JsonSerializerContext
    {
        public static readonly RequestRecordSerializerContext DefaultWithConverters;

        static RequestRecordSerializerContext()
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new AttributeValueConverter());
            options.Converters.Add(new MemoryStreamConverter());
            DefaultWithConverters = new RequestRecordSerializerContext(options);
        }
    }
}
