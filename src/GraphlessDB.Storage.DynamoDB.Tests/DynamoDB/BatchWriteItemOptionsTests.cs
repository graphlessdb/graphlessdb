/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class BatchWriteItemOptionsTests
    {
        #region Constructor Tests

        [TestMethod]
        public void ConstructorSetsBatchSizeFromParameter()
        {
            var options = new BatchWriteItemOptions(10);
            Assert.AreEqual(10, options.BatchSize);
        }

        [TestMethod]
        public void ConstructorSetsBatchSizeWithZero()
        {
            var options = new BatchWriteItemOptions(0);
            Assert.AreEqual(0, options.BatchSize);
        }

        [TestMethod]
        public void ConstructorSetsBatchSizeWithNegativeValue()
        {
            var options = new BatchWriteItemOptions(-1);
            Assert.AreEqual(-1, options.BatchSize);
        }

        [TestMethod]
        public void ConstructorSetsBatchSizeWithMaxValue()
        {
            var options = new BatchWriteItemOptions(int.MaxValue);
            Assert.AreEqual(int.MaxValue, options.BatchSize);
        }

        #endregion

        #region Default Tests

        [TestMethod]
        public void DefaultHasBatchSizeOf25()
        {
            Assert.AreEqual(25, BatchWriteItemOptions.Default.BatchSize);
        }

        [TestMethod]
        public void DefaultReturnsConsistentInstance()
        {
            var default1 = BatchWriteItemOptions.Default;
            var default2 = BatchWriteItemOptions.Default;
            Assert.AreSame(default1, default2);
        }

        #endregion

        #region Property Tests

        [TestMethod]
        public void MaxAttemptsIsNullByDefault()
        {
            var options = new BatchWriteItemOptions(25);
            Assert.IsNull(options.MaxAttempts);
        }

        [TestMethod]
        public void InitialBackoffIsDefaultByDefault()
        {
            var options = new BatchWriteItemOptions(25);
            Assert.AreEqual(default(TimeSpan), options.InitialBackoff);
        }

        [TestMethod]
        public void MaxBackoffIsDefaultByDefault()
        {
            var options = new BatchWriteItemOptions(25);
            Assert.AreEqual(default(TimeSpan), options.MaxBackoff);
        }

        [TestMethod]
        public void BackoffMultiplierIsDefaultByDefault()
        {
            var options = new BatchWriteItemOptions(25);
            Assert.AreEqual(default(double), options.BackoffMultiplier);
        }

        #endregion
    }
}
