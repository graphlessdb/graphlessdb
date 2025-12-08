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
    public enum TestStatus
    {
        Value1,
        Value2,
        Value3
    }

    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class EnumFilterTests
    {
        #region EnumFilter<T> - GetValueFilter Tests

        [TestMethod]
        public void GetValueFilterReturnsEnumFilterWithEqValue()
        {
            var filter = new EnumFilter<TestStatus> { Eq = TestStatus.Value1 };
            var result = filter.GetValueFilter();

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType<EnumFilter>(result);
            var enumFilter = (EnumFilter)result;
            Assert.AreEqual(TestStatus.Value1, enumFilter.Eq);
        }

        [TestMethod]
        public void GetValueFilterReturnsEnumFilterWithInArray()
        {
            var filter = new EnumFilter<TestStatus> { In = new[] { TestStatus.Value1, TestStatus.Value2 } };
            var result = filter.GetValueFilter();

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType<EnumFilter>(result);
            var enumFilter = (EnumFilter)result;
            Assert.IsNotNull(enumFilter.In);
            Assert.AreEqual(2, enumFilter.In.Length);
            Assert.AreEqual(TestStatus.Value1, enumFilter.In[0]);
            Assert.AreEqual(TestStatus.Value2, enumFilter.In[1]);
        }

        [TestMethod]
        public void GetValueFilterReturnsEnumFilterWithNullEq()
        {
            var filter = new EnumFilter<TestStatus> { Eq = null };
            var result = filter.GetValueFilter();

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType<EnumFilter>(result);
            var enumFilter = (EnumFilter)result;
            Assert.IsNull(enumFilter.Eq);
        }

        [TestMethod]
        public void GetValueFilterReturnsEnumFilterWithNullIn()
        {
            var filter = new EnumFilter<TestStatus> { In = null };
            var result = filter.GetValueFilter();

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType<EnumFilter>(result);
            var enumFilter = (EnumFilter)result;
            Assert.IsNull(enumFilter.In);
        }

        #endregion
    }
}
