/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class PredicateTests
    {
        #region ParseTypeName Tests

        [TestMethod]
        public void ParseTypeNameReturnsTypeNameWhenThreeHashesPresent()
        {
            var result = Predicate.ParseTypeName("prefix#namespace#TypeName");
            Assert.AreEqual("TypeName", result);
        }

        [TestMethod]
        public void ParseTypeNameReturnsTypeNameWithPropertyWhenFourHashesPresent()
        {
            var result = Predicate.ParseTypeName("prefix#namespace#TypeName#PropertyName");
            Assert.AreEqual("TypeName", result);
        }

        [TestMethod]
        public void ParseTypeNameReturnsTypeNameWithMultiplePropertiesWhenMoreHashesPresent()
        {
            var result = Predicate.ParseTypeName("prefix#namespace#TypeName#Property1#Property2");
            Assert.AreEqual("TypeName", result);
        }

        [TestMethod]
        public void ParseTypeNameReturnsRestOfStringWhenOnlyTwoHashesPresent()
        {
            var result = Predicate.ParseTypeName("prefix#namespace#TypeName");
            Assert.AreEqual("TypeName", result);
        }

        [TestMethod]
        public void ParseTypeNameHandlesEmptyTypeName()
        {
            var result = Predicate.ParseTypeName("prefix#namespace##PropertyName");
            Assert.AreEqual("", result);
        }

        #endregion

        #region ParsePropName Tests

        [TestMethod]
        public void ParsePropNameReturnsPropertyNameWhenFourHashesPresent()
        {
            var result = Predicate.ParsePropName("prefix#namespace#TypeName#PropertyName");
            Assert.AreEqual("PropertyName", result);
        }

        [TestMethod]
        public void ParsePropNameReturnsPropertyNameWithAdditionalHashWhenFiveHashesPresent()
        {
            var result = Predicate.ParsePropName("prefix#namespace#TypeName#PropertyName#Extra");
            Assert.AreEqual("PropertyName", result);
        }

        [TestMethod]
        public void ParsePropNameReturnsRestOfStringWhenOnlyThreeHashesPresent()
        {
            var result = Predicate.ParsePropName("prefix#namespace#TypeName#PropertyName");
            Assert.AreEqual("PropertyName", result);
        }

        [TestMethod]
        public void ParsePropNameHandlesEmptyPropertyName()
        {
            var result = Predicate.ParsePropName("prefix#namespace#TypeName##Extra");
            Assert.AreEqual("", result);
        }

        [TestMethod]
        public void ParsePropNameReturnsRestWhenNoFourthHash()
        {
            var result = Predicate.ParsePropName("prefix#namespace#TypeName#PropertyName");
            Assert.AreEqual("PropertyName", result);
        }

        #endregion
    }
}
