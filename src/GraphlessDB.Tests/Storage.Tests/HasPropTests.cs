/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests.Storage
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class HasPropTests
    {
        #region Constructor Validation Tests

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp(null!, "typeName", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("", "typeName", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("   ", "typeName", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenGraphNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph#name", "typeName", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenTypeNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", null!, "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenTypeNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenTypeNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "   ", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenTypeNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "type#name", "propName", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", null!, "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "   ", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyNameContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "prop#name", "propValue", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyValueIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "propName", null!, "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenPropertyValueContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "propName", "prop#value", "subject"));
        }

        [TestMethod]
        public void ConstructorThrowsWhenSubjectIsNull()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "propName", "propValue", null!));
        }

        [TestMethod]
        public void ConstructorThrowsWhenSubjectIsEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "propName", "propValue", ""));
        }

        [TestMethod]
        public void ConstructorThrowsWhenSubjectIsWhitespace()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "propName", "propValue", "   "));
        }

        [TestMethod]
        public void ConstructorThrowsWhenSubjectContainsHash()
        {
            Assert.ThrowsException<ArgumentException>(() =>
                new HasProp("graph", "typeName", "propName", "propValue", "sub#ject"));
        }

        [TestMethod]
        public void ConstructorAllowsEmptyStringPropertyValue()
        {
            var predicate = new HasProp("graph", "typeName", "propName", "", "subject");
            Assert.AreEqual("", predicate.PropertyValue);
        }

        #endregion

        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsGraphNameProperty()
        {
            var predicate = new HasProp("graph1", "typeName", "propName", "propValue", "subject");
            Assert.AreEqual("graph1", predicate.GraphName);
        }

        [TestMethod]
        public void ConstructorSetsTypeNameProperty()
        {
            var predicate = new HasProp("graph", "typeName1", "propName", "propValue", "subject");
            Assert.AreEqual("typeName1", predicate.TypeName);
        }

        [TestMethod]
        public void ConstructorSetsPropertyNameProperty()
        {
            var predicate = new HasProp("graph", "typeName", "propName1", "propValue", "subject");
            Assert.AreEqual("propName1", predicate.PropertyName);
        }

        [TestMethod]
        public void ConstructorSetsPropertyValueProperty()
        {
            var predicate = new HasProp("graph", "typeName", "propName", "propValue1", "subject");
            Assert.AreEqual("propValue1", predicate.PropertyValue);
        }

        [TestMethod]
        public void ConstructorSetsSubjectProperty()
        {
            var predicate = new HasProp("graph", "typeName", "propName", "propValue", "subject1");
            Assert.AreEqual("subject1", predicate.Subject);
        }

        #endregion

        #region ToString Tests

        [TestMethod]
        public void ToStringReturnsCorrectFormat()
        {
            var predicate = new HasProp("graph1", "typeName", "propName", "propValue", "subject");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#prop#typeName#propName#propValue#subject", result);
        }

        [TestMethod]
        public void ToStringHandlesSpecialCharacters()
        {
            var predicate = new HasProp("graph1", "typeName", "propName", "prop_value", "subject_1");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#prop#typeName#propName#prop_value#subject_1", result);
        }

        [TestMethod]
        public void ToStringHandlesEmptyPropertyValue()
        {
            var predicate = new HasProp("graph1", "typeName", "propName", "", "subject");
            var result = predicate.ToString();
            Assert.AreEqual("graph1#prop#typeName#propName##subject", result);
        }

        #endregion

        #region IsPredicate Tests

        [TestMethod]
        public void IsPredicateReturnsTrueForValidPredicate()
        {
            var result = HasProp.IsPredicate("graph1#prop#typeName#propName#propValue#subject");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsTrueForExtraPartsPredicate()
        {
            var result = HasProp.IsPredicate("graph1#prop#typeName#propName#propValue#subject#extra");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenNotEnoughParts()
        {
            var result = HasProp.IsPredicate("graph1#prop#typeName#propName#propValue");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseWhenWrongName()
        {
            var result = HasProp.IsPredicate("graph1#wrong#typeName#propName#propValue#subject");
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPredicateReturnsFalseForEmptyString()
        {
            var result = HasProp.IsPredicate("");
            Assert.IsFalse(result);
        }

        #endregion

        #region Parse Tests

        [TestMethod]
        public void ParseCreatesCorrectObject()
        {
            var result = HasProp.Parse("graph1#prop#typeName#propName#propValue#subject");
            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("typeName", result.TypeName);
            Assert.AreEqual("propName", result.PropertyName);
            Assert.AreEqual("propValue", result.PropertyValue);
            Assert.AreEqual("subject", result.Subject);
        }

        [TestMethod]
        public void ParseHandlesEmptyPropertyValue()
        {
            var result = HasProp.Parse("graph1#prop#typeName#propName##subject");
            Assert.AreEqual("", result.PropertyValue);
        }

        [TestMethod]
        public void ParseThrowsWhenNotEnoughParts()
        {
            Assert.ThrowsException<GraphlessDBOperationException>(() =>
                HasProp.Parse("graph1#prop#typeName#propName#propValue"));
        }

        [TestMethod]
        public void ParseThrowsWhenTooManyParts()
        {
            Assert.ThrowsException<GraphlessDBOperationException>(() =>
                HasProp.Parse("graph1#prop#typeName#propName#propValue#subject#extra"));
        }

        [TestMethod]
        public void ParseThrowsWhenWrongName()
        {
            Assert.ThrowsException<GraphlessDBOperationException>(() =>
                HasProp.Parse("graph1#wrong#typeName#propName#propValue#subject"));
        }

        #endregion

        #region Static Helper Methods Tests

        [TestMethod]
        public void PropertiesByTypeReturnsCorrectFormat()
        {
            var result = HasProp.PropertiesByType("graph1", "typeName");
            Assert.AreEqual("graph1#prop#typeName#", result);
        }

        [TestMethod]
        public void PropertiesByTypeAndPropertyNameReturnsCorrectFormat()
        {
            var result = HasProp.PropertiesByTypeAndPropertyName("graph1", "typeName", "propName");
            Assert.AreEqual("graph1#prop#typeName#propName#", result);
        }

        [TestMethod]
        public void PropertiesByTypePropertyNameAndValueReturnsCorrectFormat()
        {
            var result = HasProp.PropertiesByTypePropertyNameAndValue("graph1", "typeName", "propName", "propValue");
            Assert.AreEqual("graph1#prop#typeName#propName#propValue", result);
        }

        [TestMethod]
        public void PropertiesByTypePropertyNameAndValueHandlesEmptyValue()
        {
            var result = HasProp.PropertiesByTypePropertyNameAndValue("graph1", "typeName", "propName", "");
            Assert.AreEqual("graph1#prop#typeName#propName#", result);
        }

        #endregion

        #region Name Constant Test

        [TestMethod]
        public void NameConstantHasCorrectValue()
        {
            Assert.AreEqual("prop", HasProp.Name);
        }

        #endregion
    }
}
