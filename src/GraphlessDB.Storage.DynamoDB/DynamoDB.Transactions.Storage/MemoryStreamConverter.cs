/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class MemoryStreamConverter : JsonConverter<MemoryStream>
    {
        public override MemoryStream? Read(
           ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return new MemoryStream(reader.GetBytesFromBase64());
        }

        public override void Write(
            Utf8JsonWriter writer, MemoryStream value, JsonSerializerOptions options)
        {
            writer.WriteBase64StringValue(value.ToArray());
        }
    }
}
