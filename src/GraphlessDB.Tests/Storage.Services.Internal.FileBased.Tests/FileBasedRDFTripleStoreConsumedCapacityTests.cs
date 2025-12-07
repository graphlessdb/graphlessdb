/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Linq;
using System.Threading.Tasks;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services.Internal.FileBased;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.FileBased.Tests
{
    [TestClass]
    public sealed class FileBasedRDFTripleStoreConsumedCapacityTests
    {
        [TestMethod]
        public void ConstructorInitializesWithZeroCapacity()
        {
            var consumedCapacity = new FileBasedRDFTripleStoreConsumedCapacity();

            var result = consumedCapacity.GetConsumedCapacity();

            Assert.AreEqual(0d, result.CapacityUnits);
            Assert.AreEqual(0d, result.ReadCapacityUnits);
            Assert.AreEqual(0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public void AddConsumedCapacityAddsToTotal()
        {
            var consumedCapacity = new FileBasedRDFTripleStoreConsumedCapacity();
            var valueToAdd = new RDFTripleStoreConsumedCapacity(1.5d, 2.5d, 3.5d);

            consumedCapacity.AddConsumedCapacity(valueToAdd);

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(1.5d, result.CapacityUnits);
            Assert.AreEqual(2.5d, result.ReadCapacityUnits);
            Assert.AreEqual(3.5d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public void AddConsumedCapacityAddsMultipleValues()
        {
            var consumedCapacity = new FileBasedRDFTripleStoreConsumedCapacity();
            var value1 = new RDFTripleStoreConsumedCapacity(1.0d, 2.0d, 3.0d);
            var value2 = new RDFTripleStoreConsumedCapacity(0.5d, 1.5d, 2.5d);

            consumedCapacity.AddConsumedCapacity(value1);
            consumedCapacity.AddConsumedCapacity(value2);

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(1.5d, result.CapacityUnits);
            Assert.AreEqual(3.5d, result.ReadCapacityUnits);
            Assert.AreEqual(5.5d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public void ResetConsumedCapacityResetsToZero()
        {
            var consumedCapacity = new FileBasedRDFTripleStoreConsumedCapacity();
            var valueToAdd = new RDFTripleStoreConsumedCapacity(5.0d, 10.0d, 15.0d);
            consumedCapacity.AddConsumedCapacity(valueToAdd);

            consumedCapacity.ResetConsumedCapacity();

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(0d, result.CapacityUnits);
            Assert.AreEqual(0d, result.ReadCapacityUnits);
            Assert.AreEqual(0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public async Task AddConsumedCapacityIsThreadSafe()
        {
            var consumedCapacity = new FileBasedRDFTripleStoreConsumedCapacity();
            var value = new RDFTripleStoreConsumedCapacity(1.0d, 1.0d, 1.0d);

            var tasks = new Task[100];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => consumedCapacity.AddConsumedCapacity(value));
            }

            await Task.WhenAll(tasks);

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(100.0d, result.CapacityUnits);
            Assert.AreEqual(100.0d, result.ReadCapacityUnits);
            Assert.AreEqual(100.0d, result.WriteCapacityUnits);
        }
    }
}
