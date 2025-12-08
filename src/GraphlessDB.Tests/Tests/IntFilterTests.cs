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
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class IntFilterTests
    {
        [TestMethod]
        public void CanSetAndGetEqProperty()
        {
            var filter = new IntFilter { Eq = 42 };
            Assert.AreEqual(42, filter.Eq);
        }

        [TestMethod]
        public void CanSetAndGetGeProperty()
        {
            var filter = new IntFilter { Ge = 10 };
            Assert.AreEqual(10, filter.Ge);
        }

        [TestMethod]
        public void CanSetAndGetGtProperty()
        {
            var filter = new IntFilter { Gt = 5 };
            Assert.AreEqual(5, filter.Gt);
        }

        [TestMethod]
        public void CanSetAndGetLeProperty()
        {
            var filter = new IntFilter { Le = 100 };
            Assert.AreEqual(100, filter.Le);
        }

        [TestMethod]
        public void CanSetAndGetLtProperty()
        {
            var filter = new IntFilter { Lt = 50 };
            Assert.AreEqual(50, filter.Lt);
        }

        [TestMethod]
        public void PropertiesDefaultToNull()
        {
            var filter = new IntFilter();
            Assert.IsNull(filter.Eq);
            Assert.IsNull(filter.Ge);
            Assert.IsNull(filter.Gt);
            Assert.IsNull(filter.Le);
            Assert.IsNull(filter.Lt);
        }

        [TestMethod]
        public void CanSetPropertiesToNull()
        {
            var filter = new IntFilter
            {
                Eq = 1,
                Ge = 2,
                Gt = 3,
                Le = 4,
                Lt = 5
            };

            filter.Eq = null;
            filter.Ge = null;
            filter.Gt = null;
            filter.Le = null;
            filter.Lt = null;

            Assert.IsNull(filter.Eq);
            Assert.IsNull(filter.Ge);
            Assert.IsNull(filter.Gt);
            Assert.IsNull(filter.Le);
            Assert.IsNull(filter.Lt);
        }

        [TestMethod]
        public void ImplementsIValueFilter()
        {
            var filter = new IntFilter();
            Assert.IsInstanceOfType<IValueFilter>(filter);
        }
    }
}
