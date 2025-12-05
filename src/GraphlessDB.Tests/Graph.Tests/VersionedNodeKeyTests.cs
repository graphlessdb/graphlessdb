/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Tests
{
    [TestClass]
    public sealed class VersionedNodeKeyTests
    {
        [TestMethod]
        public void CanCreateVersionedNodeKeyWithValidParameters()
        {
            var id = "test-id";
            var version = 1;

            var key = new VersionedNodeKey(id, version);

            Assert.AreEqual(id, key.Id);
            Assert.AreEqual(version, key.Version);
        }

        [TestMethod]
        public void CanCreateVersionedNodeKeyWithZeroVersion()
        {
            var id = "test-id";
            var version = 0;

            var key = new VersionedNodeKey(id, version);

            Assert.AreEqual(id, key.Id);
            Assert.AreEqual(version, key.Version);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ThrowsArgumentExceptionWhenIdIsNull()
        {
            new VersionedNodeKey(null!, 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ThrowsArgumentExceptionWhenIdIsEmpty()
        {
            new VersionedNodeKey(string.Empty, 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ThrowsArgumentExceptionWhenIdIsWhitespace()
        {
            new VersionedNodeKey("   ", 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void ThrowsArgumentOutOfRangeExceptionWhenVersionIsNegative()
        {
            new VersionedNodeKey("test-id", -1);
        }
    }
}
