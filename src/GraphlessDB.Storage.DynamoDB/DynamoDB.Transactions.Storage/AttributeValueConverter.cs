/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed class AttributeValueConverter : JsonConverter<AttributeValue>
    {
        [UnconditionalSuppressMessage("RequiresUnreferencedCodeAttribute", "IL2026", Justification = "Only using primitive types.")]
        public override AttributeValue? Read(
            ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            string? propertyName = null;
            object? propertyValue = null;
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.StartObject:
                        propertyValue = propertyName switch
                        {
                            nameof(AttributeValue.M) => ((JsonConverter<Dictionary<string, AttributeValue>>)options.GetConverter(typeof(Dictionary<string, AttributeValue>))).Read(ref reader, typeof(Dictionary<string, AttributeValue>), options),
                            _ => throw new JsonException(),
                        };
                        break;
                    case JsonTokenType.PropertyName:
                        propertyName = reader.GetString();
                        if (propertyName == null)
                        {
                            throw new JsonException();
                        }
                        break;
                    case JsonTokenType.StartArray:
                        propertyValue = propertyName switch
                        {
                            nameof(AttributeValue.BS) => ((JsonConverter<List<string>>)options.GetConverter(typeof(List<string>))).Read(ref reader, typeof(List<string>), options)?.Select(v => new MemoryStream(Encoding.UTF8.GetBytes(v))).ToList(),
                            nameof(AttributeValue.NS) => ((JsonConverter<List<string>>)options.GetConverter(typeof(List<string>))).Read(ref reader, typeof(List<string>), options),
                            nameof(AttributeValue.SS) => ((JsonConverter<List<string>>)options.GetConverter(typeof(List<string>))).Read(ref reader, typeof(List<string>), options),
                            nameof(AttributeValue.L) => ((JsonConverter<List<AttributeValue>>)options.GetConverter(typeof(List<AttributeValue>))).Read(ref reader, typeof(List<AttributeValue>), options),
                            _ => throw new NotSupportedException(),
                        };

                        break;
                    case JsonTokenType.EndArray:
                        throw new JsonException();
                    case JsonTokenType.EndObject:
                        if (propertyName == null || propertyValue == null)
                        {
                            throw new JsonException();
                        }

                        return propertyName switch
                        {
                            nameof(AttributeValue.S) => AttributeValueFactory.CreateS((string)propertyValue),
                            nameof(AttributeValue.N) => AttributeValueFactory.CreateN((string)propertyValue),
                            nameof(AttributeValue.BOOL) => AttributeValueFactory.CreateBOOL((bool)propertyValue),
                            nameof(AttributeValue.B) => AttributeValueFactory.CreateB((MemoryStream)propertyValue),
                            nameof(AttributeValue.BS) => AttributeValueFactory.CreateBS((List<MemoryStream>)propertyValue),
                            nameof(AttributeValue.NS) => AttributeValueFactory.CreateNS((List<string>)propertyValue),
                            nameof(AttributeValue.SS) => AttributeValueFactory.CreateSS((List<string>)propertyValue),
                            nameof(AttributeValue.L) => AttributeValueFactory.CreateL((List<AttributeValue>)propertyValue),
                            nameof(AttributeValue.M) => AttributeValueFactory.CreateM((Dictionary<string, AttributeValue>)propertyValue),
                            nameof(AttributeValue.NULL) => AttributeValueFactory.CreateNULL((bool)propertyValue),
                            _ => throw new NotSupportedException(),
                        };
                    case JsonTokenType.String:
                        propertyValue = propertyName switch
                        {
                            nameof(AttributeValue.S) => reader.GetString(),
                            nameof(AttributeValue.N) => reader.GetString(),
                            nameof(AttributeValue.B) => new MemoryStream(Encoding.UTF8.GetBytes(reader.GetString() ?? string.Empty)),
                            _ => throw new JsonException()
                        };
                        break;
                    case JsonTokenType.True:
                        propertyValue = propertyName switch
                        {
                            nameof(AttributeValue.BOOL) => true,
                            nameof(AttributeValue.NULL) => true,
                            _ => throw new JsonException()
                        };
                        break;
                    case JsonTokenType.False:
                        propertyValue = propertyName switch
                        {
                            nameof(AttributeValue.BOOL) => false,
                            nameof(AttributeValue.NULL) => false,
                            _ => throw new JsonException()
                        };
                        break;
                    default:
                        throw new JsonException();
                }
            }

            throw new JsonException();
        }

        public override void Write(
            Utf8JsonWriter writer, AttributeValue value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            if (value.S != null)
            {
                writer.WriteStartObject();
                writer.WriteString(nameof(value.S), value.S);
                writer.WriteEndObject();
                return;
            }

            if (value.N != null)
            {
                writer.WriteStartObject();
                writer.WriteString(nameof(value.N), value.N);
                writer.WriteEndObject();
                return;
            }

            if (value.IsBOOLSet)
            {
                writer.WriteStartObject();
                writer.WriteBoolean(nameof(value.BOOL), value.BOOL);
                writer.WriteEndObject();
                return;
            }

            if (value.B != null)
            {
                writer.WriteStartObject();
                writer.WriteString(nameof(value.B), value.B.ToArray());
                writer.WriteEndObject();
                return;
            }

            if (value.BS.Count > 0)
            {
                writer.WriteStartObject();
                writer.WriteStartArray(nameof(value.BS));
                foreach (var b in value.BS)
                {
                    writer.WriteStringValue(b.ToArray());
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                return;
            }

            if (value.NS.Count > 0)
            {
                writer.WriteStartObject();
                writer.WriteStartArray(nameof(value.NS));
                foreach (var n in value.NS)
                {
                    writer.WriteStringValue(n);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                return;
            }

            if (value.SS.Count > 0)
            {
                writer.WriteStartObject();
                writer.WriteStartArray(nameof(value.SS));
                foreach (var s in value.SS)
                {
                    writer.WriteStringValue(s);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                return;
            }

            if (value.IsLSet)
            {
                writer.WriteStartObject();
                writer.WriteStartArray(nameof(value.L));
                foreach (var l in value.L)
                {
                    Write(writer, l, options);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                return;
            }

            if (value.IsMSet)
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(value.M));
                writer.WriteStartObject();
                foreach (var m in value.M)
                {
                    writer.WritePropertyName(m.Key);
                    Write(writer, m.Value, options);
                }
                writer.WriteEndObject();
                writer.WriteEndObject();
                return;
            }

            if (value.NULL)
            {
                writer.WriteStartObject();
                writer.WriteBoolean(nameof(value.NULL), value.NULL);
                writer.WriteEndObject();
                return;
            }

            throw new NotSupportedException();
        }
    }
}
