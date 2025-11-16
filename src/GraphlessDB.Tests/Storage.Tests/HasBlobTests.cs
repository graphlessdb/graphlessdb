/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Tests
{
    [TestClass]
    public sealed class HasBlobTests
    {
        [TestMethod]
        public void CanCreateWithValidParameters()
        {
            var hasBlob = new HasBlob("graph1", "type1", 1);

            Assert.AreEqual("graph1", hasBlob.GraphName);
            Assert.AreEqual("type1", hasBlob.TypeName);
            Assert.AreEqual(1, hasBlob.Version);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullGraphName()
        {
#pragma warning disable CS8625
            new HasBlob(null, "type1", 1);
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyGraphName()
        {
            new HasBlob(string.Empty, "type1", 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceGraphName()
        {
            new HasBlob("   ", "type1", 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithGraphNameContainingHash()
        {
            new HasBlob("graph#1", "type1", 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullTypeName()
        {
#pragma warning disable CS8625
            new HasBlob("graph1", null, 1);
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyTypeName()
        {
            new HasBlob("graph1", string.Empty, 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceTypeName()
        {
            new HasBlob("graph1", "   ", 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithTypeNameContainingHash()
        {
            new HasBlob("graph1", "type#1", 1);
        }

        [TestMethod]
        public void CanConvertToString()
        {
            var hasBlob = new HasBlob("graph1", "type1", 42);

            var result = hasBlob.ToString();

            Assert.AreEqual("graph1#blob#type1#42", result);
        }

        [TestMethod]
        public void CanCheckIsHasBlobReturnsTrueForValidValue()
        {
            var result = HasBlob.IsHasBlob("graph1", "graph1#$blob#type1#1");

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanCheckIsHasBlobReturnsFalseForInvalidValue()
        {
            var result = HasBlob.IsHasBlob("graph1", "graph1#other#type1#1");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanCheckIsHasBlobReturnsFalseForDifferentGraph()
        {
            var result = HasBlob.IsHasBlob("graph1", "graph2#$blob#type1#1");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanGetIsHasBlobWithType()
        {
            var result = HasBlob.IsHasBlobWithType("graph1", "type1");

            Assert.AreEqual("graph1#blob#type1#", result);
        }

        [TestMethod]
        public void CanParseValidString()
        {
            var result = HasBlob.Parse("graph1#blob#type1#42");

            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("type1", result.TypeName);
            Assert.AreEqual(42, result.Version);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotParseStringWithInsufficientParts()
        {
            HasBlob.Parse("graph1#blob#type1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotParseStringWithIncorrectPredicateName()
        {
            HasBlob.Parse("graph1#notblob#type1#42");
        }

        [TestMethod]
        [ExpectedException(typeof(FormatException))]
        public void CannotParseStringWithInvalidVersion()
        {
            HasBlob.Parse("graph1#blob#type1#notanumber");
        }
    }
}
