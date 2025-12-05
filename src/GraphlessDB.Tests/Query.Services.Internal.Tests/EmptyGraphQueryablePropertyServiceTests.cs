/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class EmptyGraphQueryablePropertyServiceTests
    {
        [TestMethod]
        public void IsQueryablePropertyReturnsFalseForAnyTypeName()
        {
            var service = new EmptyGraphQueryablePropertyService();
            var result = service.IsQueryableProperty("User", "Name");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsQueryablePropertyReturnsFalseForEmptyTypeName()
        {
            var service = new EmptyGraphQueryablePropertyService();
            var result = service.IsQueryableProperty(string.Empty, "Name");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsQueryablePropertyReturnsFalseForEmptyPropertyName()
        {
            var service = new EmptyGraphQueryablePropertyService();
            var result = service.IsQueryableProperty("User", string.Empty);
            Assert.IsFalse(result);
        }
    }
}
