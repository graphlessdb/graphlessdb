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
    public sealed class StringFilterExtensionsTests
    {
        #region IsMatch - Eq Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenEqMatches()
        {
            var filter = new StringFilter { Eq = "test" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqDoesNotMatch()
        {
            var filter = new StringFilter { Eq = "test" };
            Assert.IsFalse(filter.IsMatch("other"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenEqDoesNotMatchNull()
        {
            var filter = new StringFilter { Eq = "test" };
            Assert.IsFalse(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - Ne Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenNeDoesNotMatch()
        {
            var filter = new StringFilter { Ne = "test" };
            Assert.IsTrue(filter.IsMatch("other"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenNeMatches()
        {
            var filter = new StringFilter { Ne = "test" };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenNeDoesNotMatchNull()
        {
            var filter = new StringFilter { Ne = "test" };
            Assert.IsTrue(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - Le Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsLessThanOrEqualToLe()
        {
            var filter = new StringFilter { Le = "test" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsLessThanLe()
        {
            var filter = new StringFilter { Le = "test" };
            Assert.IsTrue(filter.IsMatch("abc"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsGreaterThanLe()
        {
            var filter = new StringFilter { Le = "test" };
            Assert.IsFalse(filter.IsMatch("zzz"));
        }

        #endregion

        #region IsMatch - Lt Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsGreaterThanLt()
        {
            var filter = new StringFilter { Lt = "abc" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanOrEqualToLt()
        {
            var filter = new StringFilter { Lt = "test" };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanLt()
        {
            var filter = new StringFilter { Lt = "test" };
            Assert.IsFalse(filter.IsMatch("abc"));
        }

        #endregion

        #region IsMatch - Ge Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsGreaterThanOrEqualToGe()
        {
            var filter = new StringFilter { Ge = "test" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsGreaterThanGe()
        {
            var filter = new StringFilter { Ge = "test" };
            Assert.IsTrue(filter.IsMatch("zzz"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanGe()
        {
            var filter = new StringFilter { Ge = "test" };
            Assert.IsFalse(filter.IsMatch("abc"));
        }

        #endregion

        #region IsMatch - Gt Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsGreaterThanGt()
        {
            var filter = new StringFilter { Gt = "abc" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanOrEqualToGt()
        {
            var filter = new StringFilter { Gt = "test" };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsLessThanGt()
        {
            var filter = new StringFilter { Gt = "test" };
            Assert.IsFalse(filter.IsMatch("abc"));
        }

        #endregion

        #region IsMatch - Contains Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueContainsSubstring()
        {
            var filter = new StringFilter { Contains = "est" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueDoesNotContainSubstring()
        {
            var filter = new StringFilter { Contains = "xyz" };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenContainsIsSetButValueIsNull()
        {
            var filter = new StringFilter { Contains = "test" };
            Assert.IsTrue(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - NotContains Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueDoesNotContainSubstring()
        {
            var filter = new StringFilter { NotContains = "xyz" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueContainsSubstring()
        {
            var filter = new StringFilter { NotContains = "est" };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenNotContainsIsSetButValueIsNull()
        {
            var filter = new StringFilter { NotContains = "test" };
            Assert.IsTrue(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - Between Tests

        [TestMethod]
        public void IsMatchThrowsNotSupportedExceptionWhenBetweenIsSet()
        {
            var filter = new StringFilter { Between = new StringRange("a", "z") };
            Assert.ThrowsException<NotSupportedException>(() => filter.IsMatch("test"));
        }

        #endregion

        #region IsMatch - BeginsWith Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueBeginsWithPrefix()
        {
            var filter = new StringFilter { BeginsWith = "te" };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueDoesNotBeginWithPrefix()
        {
            var filter = new StringFilter { BeginsWith = "xyz" };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenBeginsWithIsSetButValueIsNull()
        {
            var filter = new StringFilter { BeginsWith = "test" };
            Assert.IsTrue(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - BeginsWithAny Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueBeginsWithAnyPrefix()
        {
            var filter = new StringFilter { BeginsWithAny = new[] { "xyz", "te", "abc" } };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueDoesNotBeginWithAnyPrefix()
        {
            var filter = new StringFilter { BeginsWithAny = new[] { "xyz", "abc", "def" } };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenBeginsWithAnyIsSetButValueIsNull()
        {
            var filter = new StringFilter { BeginsWithAny = new[] { "test" } };
            Assert.IsTrue(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - In Tests

        [TestMethod]
        public void IsMatchReturnsTrueWhenValueIsInArray()
        {
            var filter = new StringFilter { In = new[] { "abc", "test", "xyz" } };
            Assert.IsTrue(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsFalseWhenValueIsNotInArray()
        {
            var filter = new StringFilter { In = new[] { "abc", "xyz" } };
            Assert.IsFalse(filter.IsMatch("test"));
        }

        [TestMethod]
        public void IsMatchReturnsTrueWhenNullValueIsInArray()
        {
            var filter = new StringFilter { In = new[] { "abc", "xyz" } };
            Assert.IsFalse(filter.IsMatch(null));
        }

        #endregion

        #region IsMatch - Empty Filter

        [TestMethod]
        public void IsMatchReturnsTrueWhenNoFiltersAreSet()
        {
            var filter = new StringFilter();
            Assert.IsTrue(filter.IsMatch("test"));
        }

        #endregion

        #region ToLowerCase Tests

        [TestMethod]
        public void ToLowerCaseReturnsNullWhenInputIsNull()
        {
            StringFilter? filter = null;
            Assert.IsNull(filter!.ToLowerCase());
        }

        [TestMethod]
        public void ToLowerCaseConvertsEqToLowerCase()
        {
            var filter = new StringFilter { Eq = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Eq);
        }

        [TestMethod]
        public void ToLowerCaseConvertsNeToLowerCase()
        {
            var filter = new StringFilter { Ne = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Ne);
        }

        [TestMethod]
        public void ToLowerCaseConvertsLeToLowerCase()
        {
            var filter = new StringFilter { Le = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Le);
        }

        [TestMethod]
        public void ToLowerCaseConvertsLtToLowerCase()
        {
            var filter = new StringFilter { Lt = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Lt);
        }

        [TestMethod]
        public void ToLowerCaseConvertsGeToLowerCase()
        {
            var filter = new StringFilter { Ge = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Ge);
        }

        [TestMethod]
        public void ToLowerCaseConvertsGtToLowerCase()
        {
            var filter = new StringFilter { Gt = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Gt);
        }

        [TestMethod]
        public void ToLowerCaseConvertsContainsToLowerCase()
        {
            var filter = new StringFilter { Contains = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Contains);
        }

        [TestMethod]
        public void ToLowerCaseConvertsNotContainsToLowerCase()
        {
            var filter = new StringFilter { NotContains = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.NotContains);
        }

        [TestMethod]
        public void ToLowerCaseConvertsBetweenToLowerCase()
        {
            var filter = new StringFilter { Between = new StringRange("AAA", "ZZZ") };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Between);
            Assert.AreEqual("aaa", result.Between.Min);
            Assert.AreEqual("zzz", result.Between.Max);
        }

        [TestMethod]
        public void ToLowerCaseHandlesNullBetween()
        {
            var filter = new StringFilter { Between = null };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNull(result.Between);
        }

        [TestMethod]
        public void ToLowerCaseConvertsBeginsWithToLowerCase()
        {
            var filter = new StringFilter { BeginsWith = "TEST" };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.BeginsWith);
        }

        [TestMethod]
        public void ToLowerCaseConvertsBeginsWithAnyToLowerCase()
        {
            var filter = new StringFilter { BeginsWithAny = new[] { "ABC", "TEST", "XYZ" } };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.BeginsWithAny);
            Assert.AreEqual(3, result.BeginsWithAny.Length);
            Assert.AreEqual("abc", result.BeginsWithAny[0]);
            Assert.AreEqual("test", result.BeginsWithAny[1]);
            Assert.AreEqual("xyz", result.BeginsWithAny[2]);
        }

        [TestMethod]
        public void ToLowerCaseHandlesNullBeginsWithAny()
        {
            var filter = new StringFilter { BeginsWithAny = null };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNull(result.BeginsWithAny);
        }

        [TestMethod]
        public void ToLowerCaseConvertsInToLowerCase()
        {
            var filter = new StringFilter { In = new[] { "ABC", "TEST", "XYZ" } };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.In);
            Assert.AreEqual(3, result.In.Length);
            Assert.AreEqual("abc", result.In[0]);
            Assert.AreEqual("test", result.In[1]);
            Assert.AreEqual("xyz", result.In[2]);
        }

        [TestMethod]
        public void ToLowerCaseHandlesNullIn()
        {
            var filter = new StringFilter { In = null };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNull(result.In);
        }

        [TestMethod]
        public void ToLowerCaseHandlesNullPropertyValues()
        {
            var filter = new StringFilter
            {
                Eq = null,
                Ne = null,
                Le = null,
                Lt = null,
                Ge = null,
                Gt = null,
                Contains = null,
                NotContains = null,
                BeginsWith = null
            };
            var result = filter.ToLowerCase();
            Assert.IsNotNull(result);
            Assert.IsNull(result.Eq);
            Assert.IsNull(result.Ne);
            Assert.IsNull(result.Le);
            Assert.IsNull(result.Lt);
            Assert.IsNull(result.Ge);
            Assert.IsNull(result.Gt);
            Assert.IsNull(result.Contains);
            Assert.IsNull(result.NotContains);
            Assert.IsNull(result.BeginsWith);
        }

        #endregion
    }
}
