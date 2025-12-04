/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class PageInfoExtensionsTests
    {
        [TestMethod]
        public void GetNullableStartCursorReturnsNullWhenStartCursorIsNull()
        {
            // Arrange
            var pageInfo = new PageInfo(false, false, null!, string.Empty);

            // Act
            var result = pageInfo.GetNullableStartCursor();

            // Assert
            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetNullableStartCursorReturnsNullWhenStartCursorIsEmpty()
        {
            // Arrange
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);

            // Act
            var result = pageInfo.GetNullableStartCursor();

            // Assert
            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetNullableStartCursorReturnsValueWhenStartCursorHasValue()
        {
            // Arrange
            var pageInfo = new PageInfo(false, false, "cursor1", string.Empty);

            // Act
            var result = pageInfo.GetNullableStartCursor();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual("cursor1", result);
        }

        [TestMethod]
        public void GetNullableEndCursorReturnsNullWhenEndCursorIsNull()
        {
            // Arrange
            var pageInfo = new PageInfo(false, false, string.Empty, null!);

            // Act
            var result = pageInfo.GetNullableEndCursor();

            // Assert
            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetNullableEndCursorReturnsNullWhenEndCursorIsEmpty()
        {
            // Arrange
            var pageInfo = new PageInfo(false, false, string.Empty, string.Empty);

            // Act
            var result = pageInfo.GetNullableEndCursor();

            // Assert
            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetNullableEndCursorReturnsValueWhenEndCursorHasValue()
        {
            // Arrange
            var pageInfo = new PageInfo(false, false, string.Empty, "cursor2");

            // Act
            var result = pageInfo.GetNullableEndCursor();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual("cursor2", result);
        }
    }
}
