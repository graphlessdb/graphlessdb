/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Text;
using System.Text.Json;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class RequestRecordSerializer : IRequestRecordSerializer
    {
        public RequestRecord Deserialize(byte[] value)
        {
            var bytes = Encoding.UTF8.GetString(value);
            var request = JsonSerializer.Deserialize<RequestRecord>(bytes, RequestRecordSerializerContext.DefaultWithConverters.RequestRecord);
            return request ?? throw new InvalidOperationException();
        }

        public byte[] Serialize(RequestRecord value)
        {
            var str = JsonSerializer.Serialize(value, RequestRecordSerializerContext.DefaultWithConverters.RequestRecord);
            return Encoding.UTF8.GetBytes(str);
        }
    }
}
