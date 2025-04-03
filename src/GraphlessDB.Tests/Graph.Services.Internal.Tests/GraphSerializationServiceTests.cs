/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class GraphSerializationServiceTests
    {
        [TestMethod]
        public void CanGetDateTimePropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString(new DateTime(2025, 1, 1));
            Assert.AreEqual("3耐638712864000000000", value);
        }

        [TestMethod]
        public void CanGetShortPropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString((short)5);
            Assert.AreEqual("3翿5", value);
        }

        [TestMethod]
        public void CanGetIntPropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString(5);
            Assert.AreEqual("3翿5", value);
        }

        [TestMethod]
        public void CanGetLongPropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString((long)5);
            Assert.AreEqual("3翿5", value);
        }

        [TestMethod]
        public void CanGetFloatPropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString((float)5.5);
            Assert.AreEqual("3翿5.5", value);
        }

        [TestMethod]
        public void CanGetDoublePropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString(5.5);
            Assert.AreEqual("3翿5.5", value);
        }

        [TestMethod]
        public void CanGetDecimalPropertyAsString()
        {
            var service = new GraphSerializationService();
            var value = service.GetPropertyAsString((decimal)5.5);
            Assert.AreEqual("3翿5.5", value);
        }
    }
}
