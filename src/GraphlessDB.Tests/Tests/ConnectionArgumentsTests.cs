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
    public sealed class ConnectionArgumentsTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void Constructor_BothFirstAndLastSpecified_ThrowsArgumentException()
        {
            // Arrange & Act & Assert
            _ = new ConnectionArguments(first: 10, last: 10);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void Constructor_NeitherFirstNorLastSpecified_ThrowsArgumentException()
        {
            // Arrange & Act & Assert
            _ = new ConnectionArguments();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void Constructor_BothAfterAndBeforeSpecified_ThrowsArgumentException()
        {
            // Arrange & Act & Assert
            _ = new ConnectionArguments(first: 10, after: "cursor1", before: "cursor2");
        }

        [TestMethod]
        public void Constructor_OnlyFirstParameter_CreatesValidInstance()
        {
            // Arrange & Act
            var args = new ConnectionArguments(first: 10);

            // Assert
            Assert.AreEqual(10, args.First);
            Assert.IsNull(args.Last);
            Assert.IsNull(args.After);
            Assert.IsNull(args.Before);
        }

        [TestMethod]
        public void Constructor_OnlyLastParameter_CreatesValidInstance()
        {
            // Arrange & Act
            var args = new ConnectionArguments(last: 10);

            // Assert
            Assert.IsNull(args.First);
            Assert.AreEqual(10, args.Last);
            Assert.IsNull(args.After);
            Assert.IsNull(args.Before);
        }

        [TestMethod]
        public void Constructor_FirstAndAfterParameters_CreatesValidInstance()
        {
            // Arrange & Act
            var args = new ConnectionArguments(first: 10, after: "cursor1");

            // Assert
            Assert.AreEqual(10, args.First);
            Assert.AreEqual("cursor1", args.After);
            Assert.IsNull(args.Last);
            Assert.IsNull(args.Before);
        }

        [TestMethod]
        public void Constructor_LastAndBeforeParameters_CreatesValidInstance()
        {
            // Arrange & Act
            var args = new ConnectionArguments(last: 10, before: "cursor1");

            // Assert
            Assert.IsNull(args.First);
            Assert.IsNull(args.After);
            Assert.AreEqual(10, args.Last);
            Assert.AreEqual("cursor1", args.Before);
        }

        #endregion

        #region Count() Method Tests

        [TestMethod]
        public void Count_WhenFirstIsSet_ReturnsFirstValue()
        {
            // Arrange
            var args = new ConnectionArguments(first: 25);

            // Act
            var count = args.Count();

            // Assert
            Assert.AreEqual(25, count);
        }

        [TestMethod]
        public void Count_WhenLastIsSet_ReturnsLastValue()
        {
            // Arrange
            var args = new ConnectionArguments(last: 30);

            // Act
            var count = args.Count();

            // Assert
            Assert.AreEqual(30, count);
        }

        #endregion

        #region Static Members Tests

        [TestMethod]
        public void Default_IsInitializedWithCorrectValues()
        {
            // Arrange & Act
            var defaultArgs = ConnectionArguments.Default;

            // Assert
            Assert.IsNotNull(defaultArgs);
            Assert.AreEqual(25, defaultArgs.First);
            Assert.IsNull(defaultArgs.Last);
            Assert.IsNull(defaultArgs.After);
            Assert.IsNull(defaultArgs.Before);
        }

        [TestMethod]
        public void FirstOne_IsInitializedWithCorrectValues()
        {
            // Arrange & Act
            var firstOne = ConnectionArguments.FirstOne;

            // Assert
            Assert.IsNotNull(firstOne);
            Assert.AreEqual(1, firstOne.First);
            Assert.IsNull(firstOne.Last);
            Assert.IsNull(firstOne.After);
            Assert.IsNull(firstOne.Before);
        }

        [TestMethod]
        public void FirstMax_IsInitializedWithCorrectValues()
        {
            // Arrange & Act
            var firstMax = ConnectionArguments.FirstMax;

            // Assert
            Assert.IsNotNull(firstMax);
            Assert.AreEqual(int.MaxValue, firstMax.First);
            Assert.IsNull(firstMax.Last);
            Assert.IsNull(firstMax.After);
            Assert.IsNull(firstMax.Before);
        }

        #endregion

        #region GetFirst() and GetLast() Tests

        [TestMethod]
        public void GetFirst_WithValueOnly_CreatesValidInstance()
        {
            // Arrange & Act
            var args = ConnectionArguments.GetFirst(10);

            // Assert
            Assert.AreEqual(10, args.First);
            Assert.IsNull(args.After);
            Assert.IsNull(args.Last);
            Assert.IsNull(args.Before);
        }

        [TestMethod]
        public void GetFirst_WithValueAndAfter_CreatesValidInstance()
        {
            // Arrange & Act
            var args = ConnectionArguments.GetFirst(15, "cursor1");

            // Assert
            Assert.AreEqual(15, args.First);
            Assert.AreEqual("cursor1", args.After);
            Assert.IsNull(args.Last);
            Assert.IsNull(args.Before);
        }

        [TestMethod]
        public void GetLast_WithValueOnly_CreatesValidInstance()
        {
            // Arrange & Act
            var args = ConnectionArguments.GetLast(20);

            // Assert
            Assert.IsNull(args.First);
            Assert.IsNull(args.After);
            Assert.AreEqual(20, args.Last);
            Assert.IsNull(args.Before);
        }

        [TestMethod]
        public void GetLast_WithValueAndBefore_CreatesValidInstance()
        {
            // Arrange & Act
            var args = ConnectionArguments.GetLast(25, "cursor2");

            // Assert
            Assert.IsNull(args.First);
            Assert.IsNull(args.After);
            Assert.AreEqual(25, args.Last);
            Assert.AreEqual("cursor2", args.Before);
        }

        #endregion
    }
}
