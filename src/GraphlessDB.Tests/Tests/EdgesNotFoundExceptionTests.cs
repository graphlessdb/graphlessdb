/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class EdgesNotFoundExceptionTests
    {
        #region Constructor Tests

        [TestMethod]
        public void DefaultConstructorCreatesExceptionWithEmptyEdgeKeys()
        {
            var exception = new EdgesNotFoundException();
            Assert.IsNotNull(exception.EdgeKeys);
            Assert.AreEqual(0, exception.EdgeKeys.Count);
        }

        [TestMethod]
        public void ConstructorWithEdgeKeysListSetsEdgeKeysProperty()
        {
            var edgeKeys = ImmutableList.Create(
                new EdgeKey("type1", "in1", "out1"),
                new EdgeKey("type2", "in2", "out2")
            );
            var exception = new EdgesNotFoundException(edgeKeys);
            Assert.AreEqual(edgeKeys, exception.EdgeKeys);
            Assert.AreEqual(2, exception.EdgeKeys.Count);
        }

        [TestMethod]
        public void ConstructorWithEmptyListSetsEmptyEdgeKeys()
        {
            var edgeKeys = ImmutableList<EdgeKey>.Empty;
            var exception = new EdgesNotFoundException(edgeKeys);
            Assert.IsNotNull(exception.EdgeKeys);
            Assert.AreEqual(0, exception.EdgeKeys.Count);
        }

        [TestMethod]
        public void ConstructorWithMessageSetsMessageProperty()
        {
            var edgeKeys = ImmutableList.Create(new EdgeKey("type1", "in1", "out1"));
            var message = "Test message";
            var exception = new EdgesNotFoundException(edgeKeys, message);
            Assert.AreEqual(message, exception.Message);
        }

        [TestMethod]
        public void ConstructorWithNullMessageSetsDefaultMessage()
        {
            var edgeKeys = ImmutableList.Create(new EdgeKey("type1", "in1", "out1"));
            var exception = new EdgesNotFoundException(edgeKeys, null);
            Assert.IsNotNull(exception.Message);
        }

        [TestMethod]
        public void ConstructorWithInnerExceptionSetsInnerExceptionProperty()
        {
            var edgeKeys = ImmutableList.Create(new EdgeKey("type1", "in1", "out1"));
            var innerException = new InvalidOperationException("Inner exception");
            var exception = new EdgesNotFoundException(edgeKeys, "Test message", innerException);
            Assert.AreEqual(innerException, exception.InnerException);
        }

        [TestMethod]
        public void ConstructorWithNullInnerExceptionSetsNullInnerException()
        {
            var edgeKeys = ImmutableList.Create(new EdgeKey("type1", "in1", "out1"));
            var exception = new EdgesNotFoundException(edgeKeys, "Test message", null);
            Assert.IsNull(exception.InnerException);
        }

        #endregion

        #region EdgeKeys Property Tests

        [TestMethod]
        public void EdgeKeysPropertyReturnsCorrectList()
        {
            var edgeKey1 = new EdgeKey("type1", "in1", "out1");
            var edgeKey2 = new EdgeKey("type2", "in2", "out2");
            var edgeKey3 = new EdgeKey("type3", "in3", "out3");
            var edgeKeys = ImmutableList.Create(edgeKey1, edgeKey2, edgeKey3);
            var exception = new EdgesNotFoundException(edgeKeys);
            Assert.AreEqual(3, exception.EdgeKeys.Count);
            Assert.AreEqual(edgeKey1, exception.EdgeKeys[0]);
            Assert.AreEqual(edgeKey2, exception.EdgeKeys[1]);
            Assert.AreEqual(edgeKey3, exception.EdgeKeys[2]);
        }

        #endregion
    }
}
