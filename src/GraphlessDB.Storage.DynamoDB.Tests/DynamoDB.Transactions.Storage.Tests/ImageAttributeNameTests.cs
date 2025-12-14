/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Storage.Tests
{
    [TestClass]
    public sealed class ImageAttributeNameTests
    {
        [TestMethod]
        public void ConstructorCreatesInstanceWithValue()
        {
            var value = "TestValue";

            var result = new ImageAttributeName(value);

            Assert.AreEqual(value, result.Value);
        }

        [TestMethod]
        public void ToStringReturnsValue()
        {
            var value = "TestValue";
            var imageAttributeName = new ImageAttributeName(value);

            var result = imageAttributeName.ToString();

            Assert.AreEqual(value, result);
        }

        [TestMethod]
        public void ImageIdHasCorrectValue()
        {
            Assert.AreEqual("_TxI", ImageAttributeName.ImageId.Value);
        }

        [TestMethod]
        public void ImageValueHasCorrectValue()
        {
            Assert.AreEqual("_TxIV", ImageAttributeName.ImageValue.Value);
        }

        [TestMethod]
        public void ValuesContainsImageId()
        {
            Assert.IsTrue(ImageAttributeName.Values.Contains(ImageAttributeName.ImageId));
        }

        [TestMethod]
        public void ValuesContainsImageValue()
        {
            Assert.IsTrue(ImageAttributeName.Values.Contains(ImageAttributeName.ImageValue));
        }

        [TestMethod]
        public void ValuesContainsExactlyTwoElements()
        {
            Assert.AreEqual(2, ImageAttributeName.Values.Count);
        }

        [TestMethod]
        public void RecordEqualityWorksForSameValue()
        {
            var name1 = new ImageAttributeName("Test");
            var name2 = new ImageAttributeName("Test");

            Assert.AreEqual(name1, name2);
        }

        [TestMethod]
        public void RecordEqualityWorksForDifferentValue()
        {
            var name1 = new ImageAttributeName("Test1");
            var name2 = new ImageAttributeName("Test2");

            Assert.AreNotEqual(name1, name2);
        }

        [TestMethod]
        public void RecordHashCodeIsSameForEqualInstances()
        {
            var name1 = new ImageAttributeName("Test");
            var name2 = new ImageAttributeName("Test");

            Assert.AreEqual(name1.GetHashCode(), name2.GetHashCode());
        }

        [TestMethod]
        public void ImageIdIsSameInstance()
        {
            var id1 = ImageAttributeName.ImageId;
            var id2 = ImageAttributeName.ImageId;

            Assert.AreSame(id1, id2);
        }

        [TestMethod]
        public void ImageValueIsSameInstance()
        {
            var value1 = ImageAttributeName.ImageValue;
            var value2 = ImageAttributeName.ImageValue;

            Assert.AreSame(value1, value2);
        }
    }
}
