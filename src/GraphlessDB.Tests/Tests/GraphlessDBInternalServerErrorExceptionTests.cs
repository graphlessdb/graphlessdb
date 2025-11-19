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
    public sealed class GraphlessDBInternalServerErrorExceptionTests
    {
        [TestMethod]
        public void ConstructorWithNoParametersCreatesException()
        {
            var exception = new GraphlessDBInternalServerErrorException();
            Assert.IsNotNull(exception);
            Assert.IsNull(exception.InnerException);
        }

        [TestMethod]
        public void ConstructorWithMessageSetsMessageProperty()
        {
            var message = "Test message";
            var exception = new GraphlessDBInternalServerErrorException(message);
            Assert.AreEqual(message, exception.Message);
            Assert.IsNull(exception.InnerException);
        }

        [TestMethod]
        public void ConstructorWithMessageAndInnerExceptionSetsProperties()
        {
            var message = "Test message";
            var innerException = new InvalidOperationException("Inner exception");
            var exception = new GraphlessDBInternalServerErrorException(message, innerException);
            Assert.AreEqual(message, exception.Message);
            Assert.AreEqual(innerException, exception.InnerException);
        }
    }
}
