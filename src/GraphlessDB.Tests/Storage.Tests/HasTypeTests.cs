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

namespace GraphlessDB.Storage.Tests
{
    [TestClass]
    public sealed class HasTypeTests
    {
        [TestMethod]
        public void CanCreateWithValidParameters()
        {
            var hasType = new HasType("graph1", "type1", "subject1");

            Assert.AreEqual("graph1", hasType.GraphName);
            Assert.AreEqual("type1", hasType.TypeName);
            Assert.AreEqual("subject1", hasType.Subject);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullGraphName()
        {
#pragma warning disable CS8625
            new HasType(null, "type1", "subject1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyGraphName()
        {
            new HasType(string.Empty, "type1", "subject1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceGraphName()
        {
            new HasType("   ", "type1", "subject1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithGraphNameContainingHash()
        {
            new HasType("graph#1", "type1", "subject1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullTypeName()
        {
#pragma warning disable CS8625
            new HasType("graph1", null, "subject1");
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptyTypeName()
        {
            new HasType("graph1", string.Empty, "subject1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceTypeName()
        {
            new HasType("graph1", "   ", "subject1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithTypeNameContainingHash()
        {
            new HasType("graph1", "type#1", "subject1");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithNullSubject()
        {
#pragma warning disable CS8625
            new HasType("graph1", "type1", null);
#pragma warning restore CS8625
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithEmptySubject()
        {
            new HasType("graph1", "type1", string.Empty);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithWhitespaceSubject()
        {
            new HasType("graph1", "type1", "   ");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void CannotCreateWithSubjectContainingHash()
        {
            new HasType("graph1", "type1", "subject#1");
        }

        [TestMethod]
        public void CanConvertToString()
        {
            var hasType = new HasType("graph1", "type1", "subject1");

            var result = hasType.ToString();

            Assert.AreEqual("graph1#type#type1#subject1", result);
        }

        [TestMethod]
        public void CanParseValidString()
        {
            var result = HasType.Parse("graph1#type#type1#subject1");

            Assert.AreEqual("graph1", result.GraphName);
            Assert.AreEqual("type1", result.TypeName);
            Assert.AreEqual("subject1", result.Subject);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void CannotParseStringWithInsufficientParts()
        {
            HasType.Parse("graph1#type#type1");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void CannotParseStringWithTooManyParts()
        {
            HasType.Parse("graph1#type#type1#subject1#extra");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void CannotParseStringWithIncorrectPredicateName()
        {
            HasType.Parse("graph1#nottype#type1#subject1");
        }

        [TestMethod]
        public void CanCheckIsPredicateReturnsTrueForValidValue()
        {
            var result = HasType.IsPredicate("graph1#type#type1#subject1");

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void CanCheckIsPredicateReturnsFalseForInvalidValue()
        {
            var result = HasType.IsPredicate("graph1#other#type1#subject1");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanCheckIsPredicateReturnsFalseForInsufficientParts()
        {
            var result = HasType.IsPredicate("graph1#type#type1");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanCheckIsPredicateReturnsFalseForTooManyParts()
        {
            var result = HasType.IsPredicate("graph1#type#type1#subject1#extra");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void CanGetByGraphName()
        {
            var result = HasType.ByGraphName("graph1");

            Assert.AreEqual("graph1#type#", result);
        }

        [TestMethod]
        public void CanGetByType()
        {
            var result = HasType.ByType("graph1", "type1");

            Assert.AreEqual("graph1#type#type1#", result);
        }
    }
}
