/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.IO;
using Amazon.DynamoDBv2.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class AttributeValueFactoryTests
    {
        #region CreateS Tests

        [TestMethod]
        public void CreateSReturnsAttributeValueWithStringSet()
        {
            var result = AttributeValueFactory.CreateS("test");
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.S);
        }

        [TestMethod]
        public void CreateSThrowsWhenStringIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateS(null!));
        }

        [TestMethod]
        public void CreateSThrowsWhenStringIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateS(string.Empty));
        }

        #endregion

        #region CreateN Tests

        [TestMethod]
        public void CreateNReturnsAttributeValueWithNumberSet()
        {
            var result = AttributeValueFactory.CreateN("123");
            Assert.IsNotNull(result);
            Assert.AreEqual("123", result.N);
        }

        [TestMethod]
        public void CreateNThrowsWhenStringIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateN(null!));
        }

        [TestMethod]
        public void CreateNThrowsWhenStringIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateN(string.Empty));
        }

        [TestMethod]
        public void CreateNThrowsWhenStringIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateN("   "));
        }

        #endregion

        #region CreateBOOL Tests

        [TestMethod]
        public void CreateBOOLReturnsAttributeValueWithTrueSet()
        {
            var result = AttributeValueFactory.CreateBOOL(true);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.BOOL);
        }

        [TestMethod]
        public void CreateBOOLReturnsAttributeValueWithFalseSet()
        {
            var result = AttributeValueFactory.CreateBOOL(false);
            Assert.IsNotNull(result);
            Assert.IsFalse(result.BOOL);
        }

        #endregion

        #region CreateNULL Tests

        [TestMethod]
        public void CreateNULLReturnsAttributeValueWithTrueSet()
        {
            var result = AttributeValueFactory.CreateNULL(true);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.NULL);
        }

        [TestMethod]
        public void CreateNULLReturnsAttributeValueWithFalseSet()
        {
            var result = AttributeValueFactory.CreateNULL(false);
            Assert.IsNotNull(result);
            Assert.IsFalse(result.NULL);
        }

        #endregion

        #region CreateB Tests

        [TestMethod]
        public void CreateBReturnsAttributeValueWithBinarySet()
        {
            using var memoryStream = new MemoryStream(new byte[] { 1, 2, 3 });
            var result = AttributeValueFactory.CreateB(memoryStream);
            Assert.IsNotNull(result);
            Assert.AreSame(memoryStream, result.B);
        }

        [TestMethod]
        public void CreateBThrowsWhenMemoryStreamIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => AttributeValueFactory.CreateB(null!));
        }

        #endregion

        #region CreateBS Tests

        [TestMethod]
        public void CreateBSReturnsAttributeValueWithBinarySetSet()
        {
            using var ms1 = new MemoryStream(new byte[] { 1, 2, 3 });
            using var ms2 = new MemoryStream(new byte[] { 4, 5, 6 });
            var list = new List<MemoryStream> { ms1, ms2 };
            var result = AttributeValueFactory.CreateBS(list);
            Assert.IsNotNull(result);
            Assert.AreSame(list, result.BS);
        }

        [TestMethod]
        public void CreateBSThrowsWhenListIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => AttributeValueFactory.CreateBS(null!));
        }

        [TestMethod]
        public void CreateBSThrowsWhenListIsEmpty()
        {
            var list = new List<MemoryStream>();
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateBS(list));
        }

        #endregion

        #region CreateNS Tests

        [TestMethod]
        public void CreateNSReturnsAttributeValueWithNumberSetSet()
        {
            var list = new List<string> { "1", "2", "3" };
            var result = AttributeValueFactory.CreateNS(list);
            Assert.IsNotNull(result);
            Assert.AreSame(list, result.NS);
        }

        [TestMethod]
        public void CreateNSThrowsWhenListIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => AttributeValueFactory.CreateNS(null!));
        }

        [TestMethod]
        public void CreateNSThrowsWhenListIsEmpty()
        {
            var list = new List<string>();
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateNS(list));
        }

        #endregion

        #region CreateSS Tests

        [TestMethod]
        public void CreateSSReturnsAttributeValueWithStringSetSet()
        {
            var list = new List<string> { "a", "b", "c" };
            var result = AttributeValueFactory.CreateSS(list);
            Assert.IsNotNull(result);
            Assert.AreSame(list, result.SS);
        }

        [TestMethod]
        public void CreateSSThrowsWhenListIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => AttributeValueFactory.CreateSS(null!));
        }

        [TestMethod]
        public void CreateSSThrowsWhenListIsEmpty()
        {
            var list = new List<string>();
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateSS(list));
        }

        #endregion

        #region CreateL Tests

        [TestMethod]
        public void CreateLReturnsAttributeValueWithListSet()
        {
            var list = new List<AttributeValue>
            {
                new AttributeValue { S = "test" },
                new AttributeValue { N = "123" }
            };
            var result = AttributeValueFactory.CreateL(list);
            Assert.IsNotNull(result);
            Assert.AreSame(list, result.L);
        }

        [TestMethod]
        public void CreateLThrowsWhenListIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => AttributeValueFactory.CreateL(null!));
        }

        [TestMethod]
        public void CreateLThrowsWhenListIsEmpty()
        {
            var list = new List<AttributeValue>();
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateL(list));
        }

        #endregion

        #region CreateM Tests

        [TestMethod]
        public void CreateMReturnsAttributeValueWithMapSet()
        {
            var dict = new Dictionary<string, AttributeValue>
            {
                { "key1", new AttributeValue { S = "value1" } },
                { "key2", new AttributeValue { N = "123" } }
            };
            var result = AttributeValueFactory.CreateM(dict);
            Assert.IsNotNull(result);
            Assert.AreSame(dict, result.M);
        }

        [TestMethod]
        public void CreateMThrowsWhenDictionaryIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => AttributeValueFactory.CreateM(null!));
        }

        [TestMethod]
        public void CreateMThrowsWhenDictionaryIsEmpty()
        {
            var dict = new Dictionary<string, AttributeValue>();
            Assert.ThrowsException<ArgumentException>(() => AttributeValueFactory.CreateM(dict));
        }

        #endregion
    }
}
