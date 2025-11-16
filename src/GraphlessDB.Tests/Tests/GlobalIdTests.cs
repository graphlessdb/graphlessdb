/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class GlobalIdTests
    {
        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsTypeNameProperty()
        {
            var globalId = new GlobalId("User", "123");
            Assert.AreEqual("User", globalId.TypeName);
        }

        [TestMethod]
        public void ConstructorSetsIdProperty()
        {
            var globalId = new GlobalId("User", "123");
            Assert.AreEqual("123", globalId.Id);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsBase64EncodedString()
        {
            var globalId = new GlobalId("User", "123");
            var result = globalId.ToString();
            Assert.AreEqual("VXNlciMxMjM=", result);
        }

        [TestMethod]
        public void ToStringHandlesSpecialCharacters()
        {
            var globalId = new GlobalId("User", "abc-def_123");
            var result = globalId.ToString();
            Assert.AreEqual("VXNlciNhYmMtZGVmXzEyMw==", result);
        }

        #endregion

        #region Get<T> Tests

        [TestMethod]
        public void GetCreatesGlobalIdFromType()
        {
            var result = GlobalId.Get<User>("456");
            Assert.AreEqual("VXNlciM0NTY=", result);
        }

        [TestMethod]
        public void GetUsesTypeNameFromGenericParameter()
        {
            var result = GlobalId.Get<Car>("789");
            Assert.AreEqual("Q2FyIzc4OQ==", result);
        }

        #endregion

        #region ParseId<T> Tests

        [TestMethod]
        public void ParseIdExtractsIdFromValidGlobalId()
        {
            var globalId = "VXNlciMxMjM="; // "User#123"
            var result = GlobalId.ParseId<User>(globalId);
            Assert.AreEqual("123", result);
        }

        [TestMethod]
        public void ParseIdExtractsIdWithSpecialCharacters()
        {
            var globalId = "VXNlciNhYmMtZGVmXzEyMw=="; // "User#abc-def_123"
            var result = GlobalId.ParseId<User>(globalId);
            Assert.AreEqual("abc-def_123", result);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ParseIdThrowsWhenTypeMismatch()
        {
            var globalId = "VXNlciMxMjM="; // "User#123"
            GlobalId.ParseId<Car>(globalId);
        }

        [TestMethod]
        public void ParseIdThrowsWithCorrectMessageWhenTypeMismatch()
        {
            var globalId = "VXNlciMxMjM="; // "User#123"
            try
            {
                GlobalId.ParseId<Car>(globalId);
                Assert.Fail("Expected ArgumentException");
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual("Global id type mismatch", ex.Message);
            }
        }

        #endregion

        #region Parse Tests

        [TestMethod]
        public void ParseReturnsGlobalIdFromValidString()
        {
            var value = "VXNlciMxMjM="; // "User#123"
            var result = GlobalId.Parse(value);
            Assert.AreEqual("User", result.TypeName);
            Assert.AreEqual("123", result.Id);
        }

        [TestMethod]
        public void ParseHandlesComplexIds()
        {
            var value = "Q2FyI2FiYy1kZWZfMTIz"; // "Car#abc-def_123"
            var result = GlobalId.Parse(value);
            Assert.AreEqual("Car", result.TypeName);
            Assert.AreEqual("abc-def_123", result.Id);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ParseThrowsWhenNoDelimiter()
        {
            var value = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("User123")); // No # delimiter
            GlobalId.Parse(value);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ParseThrowsWhenMultipleDelimiters()
        {
            var value = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("User#123#extra")); // Too many # delimiters
            GlobalId.Parse(value);
        }

        [TestMethod]
        public void ParseThrowsWithCorrectMessageWhenInvalidFormat()
        {
            var value = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("User123")); // No # delimiter
            try
            {
                GlobalId.Parse(value);
                Assert.Fail("Expected ArgumentException");
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual("Could not parse global id", ex.Message);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(FormatException))]
        public void ParseThrowsWhenInvalidBase64()
        {
            GlobalId.Parse("not-valid-base64!");
        }

        #endregion

        #region Round-trip Tests

        [TestMethod]
        public void RoundTripThroughToStringAndParse()
        {
            var original = new GlobalId("Product", "xyz-789");
            var encoded = original.ToString();
            var decoded = GlobalId.Parse(encoded);
            Assert.AreEqual(original.TypeName, decoded.TypeName);
            Assert.AreEqual(original.Id, decoded.Id);
        }

        [TestMethod]
        public void RoundTripThroughGetAndParseId()
        {
            var originalId = "test-id-123";
            var encoded = GlobalId.Get<User>(originalId);
            var decodedId = GlobalId.ParseId<User>(encoded);
            Assert.AreEqual(originalId, decodedId);
        }

        #endregion
    }
}
