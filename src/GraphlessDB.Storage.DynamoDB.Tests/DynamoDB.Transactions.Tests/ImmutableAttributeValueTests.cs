/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.Collections.Immutable;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class ImmutableAttributeValueTests
    {
        [TestMethod]
        public void CanCreateFromStringAttributeValue()
        {
            var attributeValue = AttributeValueFactory.CreateS("test");
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.AreEqual("test", immutable.S);
            Assert.IsNull(immutable.N);
            Assert.IsNull(immutable.B);
            Assert.IsNull(immutable.M);
            Assert.IsNull(immutable.NS);
            Assert.IsNull(immutable.SS);
            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CanCreateFromNumberAttributeValue()
        {
            var attributeValue = AttributeValueFactory.CreateN("123.45");
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNull(immutable.S);
            Assert.AreEqual("123.45", immutable.N);
            Assert.IsNull(immutable.B);
            Assert.IsNull(immutable.M);
            Assert.IsNull(immutable.NS);
            Assert.IsNull(immutable.SS);
            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CanCreateFromBinaryAttributeValue()
        {
            var bytes = new byte[] { 1, 2, 3, 4, 5 };
            var attributeValue = AttributeValueFactory.CreateB(new MemoryStream(bytes));
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNull(immutable.S);
            Assert.IsNull(immutable.N);
            Assert.IsNotNull(immutable.B);
            CollectionAssert.AreEqual(bytes, immutable.B.Items.ToArray());
            Assert.IsNull(immutable.M);
            Assert.IsNull(immutable.NS);
            Assert.IsNull(immutable.SS);
            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CanCreateFromMapAttributeValue()
        {
            var map = new Dictionary<string, AttributeValue>
            {
                { "key1", AttributeValueFactory.CreateS("value1") },
                { "key2", AttributeValueFactory.CreateN("42") }
            };
            var attributeValue = AttributeValueFactory.CreateM(map);
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNull(immutable.S);
            Assert.IsNull(immutable.N);
            Assert.IsNull(immutable.B);
            Assert.IsNotNull(immutable.M);
            Assert.AreEqual(2, immutable.M.Items.Count);
            Assert.IsNull(immutable.NS);
            Assert.IsNull(immutable.SS);
            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CanCreateFromNumberSetAttributeValue()
        {
            var numbers = new List<string> { "1", "2", "3" };
            var attributeValue = AttributeValueFactory.CreateNS(numbers);
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNull(immutable.S);
            Assert.IsNull(immutable.N);
            Assert.IsNull(immutable.B);
            Assert.IsNull(immutable.M);
            Assert.IsNotNull(immutable.NS);
            CollectionAssert.AreEqual(numbers, immutable.NS.Items.ToList());
            Assert.IsNull(immutable.SS);
            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CanCreateFromStringSetAttributeValue()
        {
            var strings = new List<string> { "a", "b", "c" };
            var attributeValue = AttributeValueFactory.CreateSS(strings);
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNull(immutable.S);
            Assert.IsNull(immutable.N);
            Assert.IsNull(immutable.B);
            Assert.IsNull(immutable.M);
            Assert.IsNull(immutable.NS);
            Assert.IsNotNull(immutable.SS);
            CollectionAssert.AreEqual(strings, immutable.SS.Items.ToList());
            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CanCreateFromBinarySetAttributeValue()
        {
            var bytes1 = new byte[] { 1, 2, 3 };
            var bytes2 = new byte[] { 4, 5, 6 };
            var binarySet = new List<MemoryStream>
            {
                new MemoryStream(bytes1),
                new MemoryStream(bytes2)
            };
            var attributeValue = AttributeValueFactory.CreateBS(binarySet);
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNull(immutable.S);
            Assert.IsNull(immutable.N);
            Assert.IsNull(immutable.B);
            Assert.IsNull(immutable.M);
            Assert.IsNull(immutable.NS);
            Assert.IsNull(immutable.SS);
            Assert.IsNotNull(immutable.BS);
            Assert.AreEqual(2, immutable.BS.Items.Count);
            CollectionAssert.AreEqual(bytes1, immutable.BS.Items[0].Items.ToArray());
            CollectionAssert.AreEqual(bytes2, immutable.BS.Items[1].Items.ToArray());
        }

        [TestMethod]
        public void CanConvertStringToAttributeValue()
        {
            var immutable = new ImmutableAttributeValue("test", null, null, null, null, null, null);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.AreEqual("test", attributeValue.S);
        }

        [TestMethod]
        public void CanConvertNumberToAttributeValue()
        {
            var immutable = new ImmutableAttributeValue(null, "123", null, null, null, null, null);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.AreEqual("123", attributeValue.N);
        }

        [TestMethod]
        public void CanConvertBinaryToAttributeValue()
        {
            var bytes = new byte[] { 1, 2, 3 };
            var b = bytes.ToImmutableList().ToImmutableListSequence();
            var immutable = new ImmutableAttributeValue(null, null, b, null, null, null, null);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.IsNotNull(attributeValue.B);
            CollectionAssert.AreEqual(bytes, attributeValue.B.ToArray());
        }

        [TestMethod]
        public void CanConvertMapToAttributeValue()
        {
            var items = new List<RecordTuple<string, ImmutableAttributeValue>>
            {
                new RecordTuple<string, ImmutableAttributeValue>("key1", new ImmutableAttributeValue("value1", null, null, null, null, null, null))
            };
            var m = items.ToImmutableList().ToImmutableListSequence();
            var immutable = new ImmutableAttributeValue(null, null, null, m, null, null, null);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.IsNotNull(attributeValue.M);
            Assert.AreEqual(1, attributeValue.M.Count);
            Assert.IsTrue(attributeValue.M.ContainsKey("key1"));
            Assert.AreEqual("value1", attributeValue.M["key1"].S);
        }

        [TestMethod]
        public void CanConvertNumberSetToAttributeValue()
        {
            var numbers = new List<string> { "1", "2", "3" };
            var ns = numbers.ToImmutableList().ToImmutableListSequence();
            var immutable = new ImmutableAttributeValue(null, null, null, null, ns, null, null);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.IsNotNull(attributeValue.NS);
            CollectionAssert.AreEqual(numbers, attributeValue.NS.ToList());
        }

        [TestMethod]
        public void CanConvertStringSetToAttributeValue()
        {
            var strings = new List<string> { "a", "b", "c" };
            var ss = strings.ToImmutableList().ToImmutableListSequence();
            var immutable = new ImmutableAttributeValue(null, null, null, null, null, ss, null);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.IsNotNull(attributeValue.SS);
            CollectionAssert.AreEqual(strings, attributeValue.SS.ToList());
        }

        [TestMethod]
        public void CanConvertBinarySetToAttributeValue()
        {
            var bytes1 = new byte[] { 1, 2, 3 };
            var bytes2 = new byte[] { 4, 5, 6 };
            var binarySet = new List<ImmutableListSequence<byte>>
            {
                bytes1.ToImmutableList().ToImmutableListSequence(),
                bytes2.ToImmutableList().ToImmutableListSequence()
            };
            var bs = binarySet.ToImmutableList().ToImmutableListSequence();
            var immutable = new ImmutableAttributeValue(null, null, null, null, null, null, bs);
            var attributeValue = immutable.ToAttributeValue();

            Assert.IsNotNull(attributeValue);
            Assert.IsNotNull(attributeValue.BS);
            Assert.AreEqual(2, attributeValue.BS.Count);
            CollectionAssert.AreEqual(bytes1, attributeValue.BS[0].ToArray());
            CollectionAssert.AreEqual(bytes2, attributeValue.BS[1].ToArray());
        }

        [TestMethod]
        public void ToAttributeValueThrowsWhenNoValueSet()
        {
            var immutable = new ImmutableAttributeValue(null, null, null, null, null, null, null);
            Assert.ThrowsException<InvalidOperationException>(() => immutable.ToAttributeValue());
        }

        [TestMethod]
        public void CanRoundTripStringAttributeValue()
        {
            var original = AttributeValueFactory.CreateS("test");
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            Assert.AreEqual(original.S, result.S);
        }

        [TestMethod]
        public void CanRoundTripNumberAttributeValue()
        {
            var original = AttributeValueFactory.CreateN("456.78");
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            Assert.AreEqual(original.N, result.N);
        }

        [TestMethod]
        public void CanRoundTripBinaryAttributeValue()
        {
            var bytes = new byte[] { 10, 20, 30, 40 };
            var original = AttributeValueFactory.CreateB(new MemoryStream(bytes));
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            CollectionAssert.AreEqual(bytes, result.B.ToArray());
        }

        [TestMethod]
        public void CanRoundTripMapAttributeValue()
        {
            var map = new Dictionary<string, AttributeValue>
            {
                { "strKey", AttributeValueFactory.CreateS("strValue") },
                { "numKey", AttributeValueFactory.CreateN("99") }
            };
            var original = AttributeValueFactory.CreateM(map);
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            Assert.AreEqual(2, result.M.Count);
            Assert.AreEqual("strValue", result.M["strKey"].S);
            Assert.AreEqual("99", result.M["numKey"].N);
        }

        [TestMethod]
        public void CanRoundTripNumberSetAttributeValue()
        {
            var numbers = new List<string> { "10", "20", "30" };
            var original = AttributeValueFactory.CreateNS(numbers);
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            CollectionAssert.AreEqual(numbers, result.NS.ToList());
        }

        [TestMethod]
        public void CanRoundTripStringSetAttributeValue()
        {
            var strings = new List<string> { "x", "y", "z" };
            var original = AttributeValueFactory.CreateSS(strings);
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            CollectionAssert.AreEqual(strings, result.SS.ToList());
        }

        [TestMethod]
        public void CanRoundTripBinarySetAttributeValue()
        {
            var bytes1 = new byte[] { 11, 22 };
            var bytes2 = new byte[] { 33, 44 };
            var binarySet = new List<MemoryStream>
            {
                new MemoryStream(bytes1),
                new MemoryStream(bytes2)
            };
            var original = AttributeValueFactory.CreateBS(binarySet);
            var immutable = ImmutableAttributeValue.Create(original);
            var result = immutable.ToAttributeValue();

            Assert.AreEqual(2, result.BS.Count);
            CollectionAssert.AreEqual(bytes1, result.BS[0].ToArray());
            CollectionAssert.AreEqual(bytes2, result.BS[1].ToArray());
        }

        [TestMethod]
        public void CreateHandlesNestedMapAttributeValue()
        {
            var nestedMap = new Dictionary<string, AttributeValue>
            {
                { "inner", AttributeValueFactory.CreateS("innerValue") }
            };
            var outerMap = new Dictionary<string, AttributeValue>
            {
                { "outer", AttributeValueFactory.CreateM(nestedMap) }
            };
            var attributeValue = AttributeValueFactory.CreateM(outerMap);
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable);
            Assert.IsNotNull(immutable.M);
            Assert.AreEqual(1, immutable.M.Items.Count);
            var outerItem = immutable.M.Items[0];
            Assert.AreEqual("outer", outerItem.Item1);
            Assert.IsNotNull(outerItem.Item2.M);
        }

        [TestMethod]
        public void CreateOrdersMapKeysByName()
        {
            var map = new Dictionary<string, AttributeValue>
            {
                { "zebra", AttributeValueFactory.CreateS("z") },
                { "alpha", AttributeValueFactory.CreateS("a") },
                { "middle", AttributeValueFactory.CreateS("m") }
            };
            var attributeValue = AttributeValueFactory.CreateM(map);
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNotNull(immutable.M);
            var keys = immutable.M.Items.Select(x => x.Item1).ToList();
            CollectionAssert.AreEqual(new List<string> { "alpha", "middle", "zebra" }, keys);
        }

        [TestMethod]
        public void CreateHandlesEmptyNumberSet()
        {
            var attributeValue = new AttributeValue { NS = new List<string>() };
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNull(immutable.NS);
        }

        [TestMethod]
        public void CreateHandlesEmptyStringSet()
        {
            var attributeValue = new AttributeValue { SS = new List<string>() };
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNull(immutable.SS);
        }

        [TestMethod]
        public void CreateHandlesEmptyBinarySet()
        {
            var attributeValue = new AttributeValue { BS = new List<MemoryStream>() };
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNull(immutable.BS);
        }

        [TestMethod]
        public void CreateHandlesNullNumberSet()
        {
            var attributeValue = new AttributeValue { NS = null };
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNull(immutable.NS);
        }

        [TestMethod]
        public void CreateHandlesNullStringSet()
        {
            var attributeValue = new AttributeValue { SS = null };
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNull(immutable.SS);
        }

        [TestMethod]
        public void CreateHandlesNullBinarySet()
        {
            var attributeValue = new AttributeValue { BS = null };
            var immutable = ImmutableAttributeValue.Create(attributeValue);

            Assert.IsNull(immutable.BS);
        }
    }
}
