/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Text.Json;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Storage.Tests
{
    [TestClass]
    [UnconditionalSuppressMessage("RequiresUnreferencedCodeAttribute", "IL2026", Justification = "Only using MemoryStream types.")]
    public sealed class MemoryStreamConverterTests
    {
        private static JsonSerializerOptions GetOptionsWithConverter()
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new MemoryStreamConverter());
            return options;
        }

        [TestMethod]
        public void WriteSerializesMemoryStreamAsBase64String()
        {
            var converter = new MemoryStreamConverter();
            var data = new byte[] { 1, 2, 3, 4, 5 };
            var memoryStream = new MemoryStream(data);
            var options = GetOptionsWithConverter();

            var json = JsonSerializer.Serialize(memoryStream, options);
            var expectedBase64 = Convert.ToBase64String(data);

            Assert.AreEqual($"\"{expectedBase64}\"", json);
        }

        [TestMethod]
        public void ReadDeserializesBase64StringToMemoryStream()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            var base64 = Convert.ToBase64String(data);
            var json = $"\"{base64}\"";
            var options = GetOptionsWithConverter();

            var result = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(data, result.ToArray());
        }

        [TestMethod]
        public void WriteAndReadRoundTripPreservesData()
        {
            var originalData = new byte[] { 10, 20, 30, 40, 50, 60 };
            var originalStream = new MemoryStream(originalData);
            var options = GetOptionsWithConverter();

            var json = JsonSerializer.Serialize(originalStream, options);
            var deserializedStream = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.IsNotNull(deserializedStream);
            CollectionAssert.AreEqual(originalData, deserializedStream.ToArray());
        }

        [TestMethod]
        public void WriteHandlesEmptyMemoryStream()
        {
            var emptyStream = new MemoryStream();
            var options = GetOptionsWithConverter();

            var json = JsonSerializer.Serialize(emptyStream, options);

            Assert.AreEqual("\"\"", json);
        }

        [TestMethod]
        public void ReadHandlesEmptyBase64String()
        {
            var json = "\"\"";
            var options = GetOptionsWithConverter();

            var result = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Length);
        }

        [TestMethod]
        public void WriteHandlesLargeMemoryStream()
        {
            var largeData = new byte[10000];
            for (int i = 0; i < largeData.Length; i++)
            {
                largeData[i] = (byte)(i % 256);
            }
            var largeStream = new MemoryStream(largeData);
            var options = GetOptionsWithConverter();

            var json = JsonSerializer.Serialize(largeStream, options);
            var deserializedStream = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.IsNotNull(deserializedStream);
            CollectionAssert.AreEqual(largeData, deserializedStream.ToArray());
        }

        [TestMethod]
        public void WriteHandlesMemoryStreamWithNonZeroPosition()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            var memoryStream = new MemoryStream(data);
            memoryStream.Position = 3;
            var options = GetOptionsWithConverter();

            var json = JsonSerializer.Serialize(memoryStream, options);
            var expectedBase64 = Convert.ToBase64String(data);

            Assert.AreEqual($"\"{expectedBase64}\"", json);
        }

        [TestMethod]
        public void ReadReturnsNewMemoryStreamInstance()
        {
            var data = new byte[] { 1, 2, 3 };
            var base64 = Convert.ToBase64String(data);
            var json = $"\"{base64}\"";
            var options = GetOptionsWithConverter();

            var result1 = JsonSerializer.Deserialize<MemoryStream>(json, options);
            var result2 = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.AreNotSame(result1, result2);
        }

        [TestMethod]
        public void WriteHandlesBinaryData()
        {
            var binaryData = new byte[] { 0x00, 0xFF, 0x7F, 0x80, 0xAB };
            var memoryStream = new MemoryStream(binaryData);
            var options = GetOptionsWithConverter();

            var json = JsonSerializer.Serialize(memoryStream, options);
            var deserializedStream = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.IsNotNull(deserializedStream);
            CollectionAssert.AreEqual(binaryData, deserializedStream.ToArray());
        }

        [TestMethod]
        public void ReadHandlesValidBase64WithPadding()
        {
            var data = new byte[] { 1, 2 };
            var base64 = Convert.ToBase64String(data);
            var json = $"\"{base64}\"";
            var options = GetOptionsWithConverter();

            var result = JsonSerializer.Deserialize<MemoryStream>(json, options);

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(data, result.ToArray());
        }

        [TestMethod]
        public void ConverterCanBeUsedInComplexObject()
        {
            var options = GetOptionsWithConverter();
            var data = new byte[] { 1, 2, 3 };
            var memoryStream = new MemoryStream(data);
            var wrapper = new { Stream = memoryStream };

            var json = JsonSerializer.Serialize(wrapper, options);
            var deserialized = JsonSerializer.Deserialize<JsonElement>(json, options);

            var streamBase64 = deserialized.GetProperty("Stream").GetString();
            Assert.AreEqual(Convert.ToBase64String(data), streamBase64);
        }
    }
}
