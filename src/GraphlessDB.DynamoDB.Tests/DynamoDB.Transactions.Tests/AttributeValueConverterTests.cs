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
using System.Text;
using System.Text.Json;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    [UnconditionalSuppressMessage("RequiresUnreferencedCodeAttribute", "IL2026", Justification = "Only using primitive types.")]
    public sealed class AttributeValueConverterTests
    {
        private static JsonSerializerOptions CreateOptions()
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new AttributeValueConverter());
            return options;
        }

        [TestMethod]
        public void CanSerializeAndDeserializeStringAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateS("test string");

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual("test string", deserialized.S);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeNumberAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateN("123.45");

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual("123.45", deserialized.N);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeBooleanTrueAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateBOOL(true);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.IsTrue(deserialized.BOOL);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeBooleanFalseAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateBOOL(false);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.IsFalse(deserialized.BOOL);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeBinaryAttributeValue()
        {
            var options = CreateOptions();
            var data = Encoding.UTF8.GetBytes("binary data \n\t\u0123");
            var attributeValue = AttributeValueFactory.CreateB(new MemoryStream(data));

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.IsNotNull(deserialized.B);
            CollectionAssert.AreEqual(data, deserialized.B.ToArray());
        }

        [TestMethod]
        public void CanSerializeAndDeserializeBinarySetAttributeValue()
        {
            var options = CreateOptions();
            var data1 = Encoding.UTF8.GetBytes("data1");
            var data2 = Encoding.UTF8.GetBytes("data2 \n\t");
            var attributeValue = AttributeValueFactory.CreateBS(
            [
                new MemoryStream(data1),
                new MemoryStream(data2)
            ]);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(2, deserialized.BS.Count);
            CollectionAssert.AreEqual(data1, deserialized.BS[0].ToArray());
            CollectionAssert.AreEqual(data2, deserialized.BS[1].ToArray());
        }

        [TestMethod]
        public void CanSerializeAndDeserializeNumberSetAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateNS(["123", "456.78", "-99"]);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(3, deserialized.NS.Count);
            Assert.AreEqual("123", deserialized.NS[0]);
            Assert.AreEqual("456.78", deserialized.NS[1]);
            Assert.AreEqual("-99", deserialized.NS[2]);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeStringSetAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateSS(["value1", "value2", "value3"]);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(3, deserialized.SS.Count);
            Assert.AreEqual("value1", deserialized.SS[0]);
            Assert.AreEqual("value2", deserialized.SS[1]);
            Assert.AreEqual("value3", deserialized.SS[2]);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeListAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateL(
            [
                AttributeValueFactory.CreateS("string"),
                AttributeValueFactory.CreateN("123"),
                AttributeValueFactory.CreateBOOL(true)
            ]);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(3, deserialized.L.Count);
            Assert.AreEqual("string", deserialized.L[0].S);
            Assert.AreEqual("123", deserialized.L[1].N);
            Assert.IsTrue(deserialized.L[2].BOOL);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeMapAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateM(new Dictionary<string, AttributeValue>
            {
                { "key1", AttributeValueFactory.CreateS("value1") },
                { "key2", AttributeValueFactory.CreateN("42") },
                { "key3", AttributeValueFactory.CreateBOOL(false) }
            });

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(3, deserialized.M.Count);
            Assert.AreEqual("value1", deserialized.M["key1"].S);
            Assert.AreEqual("42", deserialized.M["key2"].N);
            Assert.IsFalse(deserialized.M["key3"].BOOL);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeNullAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateNULL(true);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.IsTrue(deserialized.NULL);
        }


        [TestMethod]
        public void CanSerializeNullValue()
        {
            var options = CreateOptions();
            AttributeValue? attributeValue = null;

            var json = JsonSerializer.Serialize(attributeValue, options);

            Assert.AreEqual("null", json);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeNestedListAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateL(
            [
                AttributeValueFactory.CreateL(
                [
                    AttributeValueFactory.CreateS("nested"),
                    AttributeValueFactory.CreateN("1")
                ]),
                AttributeValueFactory.CreateL(
                [
                    AttributeValueFactory.CreateS("nested2"),
                    AttributeValueFactory.CreateN("2")
                ])
            ]);

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(2, deserialized.L.Count);
            Assert.AreEqual(2, deserialized.L[0].L.Count);
            Assert.AreEqual("nested", deserialized.L[0].L[0].S);
            Assert.AreEqual("1", deserialized.L[0].L[1].N);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeNestedMapAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateM(new Dictionary<string, AttributeValue>
            {
                {
                    "nested",
                    AttributeValueFactory.CreateM(new Dictionary<string, AttributeValue>
                    {
                        { "innerKey", AttributeValueFactory.CreateS("innerValue") }
                    })
                }
            });

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(1, deserialized.M.Count);
            Assert.IsNotNull(deserialized.M["nested"].M);
            Assert.AreEqual("innerValue", deserialized.M["nested"].M["innerKey"].S);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionWhenNotStartObject()
        {
            var options = CreateOptions();
            var json = "\"not an object\"";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionWhenPropertyNameIsNull()
        {
            var options = CreateOptions();
            var json = "{null: \"value\"}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnexpectedStartObjectProperty()
        {
            var options = CreateOptions();
            var json = "{\"S\": {}}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void ReadThrowsNotSupportedExceptionForUnknownArrayProperty()
        {
            var options = CreateOptions();
            var json = "{\"UNKNOWN\": []}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnexpectedEndArray()
        {
            var options = CreateOptions();
            var json = "[1, 2, 3]";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionWhenPropertyValueIsNull()
        {
            var options = CreateOptions();
            var json = "{}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnknownPropertyName()
        {
            var options = CreateOptions();
            var json = "{\"UNKNOWN\": \"value\"}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnexpectedStringProperty()
        {
            var options = CreateOptions();
            var json = "{\"BOOL\": \"not a boolean\"}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnexpectedTrueProperty()
        {
            var options = CreateOptions();
            var json = "{\"S\": true}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnexpectedFalseProperty()
        {
            var options = CreateOptions();
            var json = "{\"N\": false}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnexpectedTokenType()
        {
            var options = CreateOptions();
            var json = "{\"S\": 123}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void WriteThrowsNotSupportedExceptionForUnsupportedAttributeValue()
        {
            var options = CreateOptions();
            var attributeValue = new AttributeValue();

            JsonSerializer.Serialize(attributeValue, options);
        }


        [TestMethod]
        public void CanSerializeAndDeserializeComplexNestedStructure()
        {
            var options = CreateOptions();
            var attributeValue = AttributeValueFactory.CreateM(new Dictionary<string, AttributeValue>
            {
                { "string", AttributeValueFactory.CreateS("value") },
                { "number", AttributeValueFactory.CreateN("123") },
                { "bool", AttributeValueFactory.CreateBOOL(true) },
                { "null", AttributeValueFactory.CreateNULL(true) },
                { "list", AttributeValueFactory.CreateL(
                    [
                        AttributeValueFactory.CreateS("item1"),
                        AttributeValueFactory.CreateN("42")
                    ])
                },
                { "map", AttributeValueFactory.CreateM(new Dictionary<string, AttributeValue>
                    {
                        { "nested", AttributeValueFactory.CreateS("nested value") }
                    })
                },
                { "ss", AttributeValueFactory.CreateSS(["a", "b", "c"]) },
                { "ns", AttributeValueFactory.CreateNS(["1", "2", "3"]) }
            });

            var json = JsonSerializer.Serialize(attributeValue, options);
            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual("value", deserialized.M["string"].S);
            Assert.AreEqual("123", deserialized.M["number"].N);
            Assert.IsTrue(deserialized.M["bool"].BOOL);
            Assert.IsTrue(deserialized.M["null"].NULL);
            Assert.AreEqual(2, deserialized.M["list"].L.Count);
            Assert.AreEqual("nested value", deserialized.M["map"].M["nested"].S);
            Assert.AreEqual(3, deserialized.M["ss"].SS.Count);
            Assert.AreEqual(3, deserialized.M["ns"].NS.Count);
        }

        [TestMethod]
        public void CanDeserializeNullAttributeValueFalse()
        {
            var options = CreateOptions();
            var json = "{\"NULL\": false}";

            var deserialized = JsonSerializer.Deserialize<AttributeValue>(json, options);

            Assert.IsNotNull(deserialized);
            Assert.IsFalse(deserialized.NULL);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionForUnknownTypeAtEndObject()
        {
            var options = CreateOptions();
            var json = "{\"UNKNOWNTYPE\": \"value\"}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        public void WriteSerializesNullAsJsonNull()
        {
            var options = CreateOptions();
            AttributeValue? value = null;

            var json = JsonSerializer.Serialize(value, options);

            Assert.AreEqual("null", json);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionWhenInputEndsWithoutCompleteObject()
        {
            var options = CreateOptions();
            var json = "{\"S\":";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }

        [TestMethod]
        public void ConverterHandlesNullInWriteMethod()
        {
            var converter = new AttributeValueConverter();
            var options = new JsonSerializerOptions();
            using var stream = new MemoryStream();
            using var writer = new Utf8JsonWriter(stream);

            writer.WriteStartObject();
            writer.WritePropertyName("test");
            converter.Write(writer, null!, options);
            writer.WriteEndObject();
            writer.Flush();

            var json = Encoding.UTF8.GetString(stream.ToArray());
            Assert.IsTrue(json.Contains("null"));
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void ReadThrowsJsonExceptionWhenPropertyValueIsNullAtEndObject()
        {
            var options = CreateOptions();
            var json = "{\"S\": null}";

            JsonSerializer.Deserialize<AttributeValue>(json, options);
        }
    }
}
