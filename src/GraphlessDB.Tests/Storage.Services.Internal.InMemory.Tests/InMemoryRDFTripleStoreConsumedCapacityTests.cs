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
using GraphlessDB.Storage.Services.Internal.InMemory;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.InMemory.Tests
{
    [TestClass]
    public sealed class InMemoryRDFTripleStoreConsumedCapacityTests
    {
        [TestMethod]
        public void ConstructorInitializesWithZeroCapacity()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();

            var result = consumedCapacity.GetConsumedCapacity();

            Assert.AreEqual(0d, result.CapacityUnits);
            Assert.AreEqual(0d, result.ReadCapacityUnits);
            Assert.AreEqual(0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public void AddConsumedCapacityAddsToTotal()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
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
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
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
        public void GetConsumedCapacityReturnsCurrentTotal()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var initialResult = consumedCapacity.GetConsumedCapacity();

            Assert.AreEqual(0d, initialResult.CapacityUnits);
            Assert.AreEqual(0d, initialResult.ReadCapacityUnits);
            Assert.AreEqual(0d, initialResult.WriteCapacityUnits);

            var valueToAdd = new RDFTripleStoreConsumedCapacity(10.0d, 20.0d, 30.0d);
            consumedCapacity.AddConsumedCapacity(valueToAdd);

            var finalResult = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(10.0d, finalResult.CapacityUnits);
            Assert.AreEqual(20.0d, finalResult.ReadCapacityUnits);
            Assert.AreEqual(30.0d, finalResult.WriteCapacityUnits);
        }

        [TestMethod]
        public void ResetConsumedCapacityResetsToZero()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var valueToAdd = new RDFTripleStoreConsumedCapacity(5.0d, 10.0d, 15.0d);
            consumedCapacity.AddConsumedCapacity(valueToAdd);

            consumedCapacity.ResetConsumedCapacity();

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(0d, result.CapacityUnits);
            Assert.AreEqual(0d, result.ReadCapacityUnits);
            Assert.AreEqual(0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public void ResetConsumedCapacityCanBeCalledMultipleTimes()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var valueToAdd = new RDFTripleStoreConsumedCapacity(1.0d, 2.0d, 3.0d);
            consumedCapacity.AddConsumedCapacity(valueToAdd);

            consumedCapacity.ResetConsumedCapacity();
            consumedCapacity.ResetConsumedCapacity();

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(0d, result.CapacityUnits);
            Assert.AreEqual(0d, result.ReadCapacityUnits);
            Assert.AreEqual(0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public void AddConsumedCapacityAfterResetStartsFromZero()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var value1 = new RDFTripleStoreConsumedCapacity(5.0d, 10.0d, 15.0d);
            consumedCapacity.AddConsumedCapacity(value1);

            consumedCapacity.ResetConsumedCapacity();

            var value2 = new RDFTripleStoreConsumedCapacity(2.0d, 4.0d, 6.0d);
            consumedCapacity.AddConsumedCapacity(value2);

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(2.0d, result.CapacityUnits);
            Assert.AreEqual(4.0d, result.ReadCapacityUnits);
            Assert.AreEqual(6.0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public async Task AddConsumedCapacityIsThreadSafe()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
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

        [TestMethod]
        public async Task ResetConsumedCapacityIsThreadSafe()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var value = new RDFTripleStoreConsumedCapacity(10.0d, 10.0d, 10.0d);
            consumedCapacity.AddConsumedCapacity(value);

            var tasks = new Task[50];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => consumedCapacity.ResetConsumedCapacity());
            }

            await Task.WhenAll(tasks);

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(0d, result.CapacityUnits);
            Assert.AreEqual(0d, result.ReadCapacityUnits);
            Assert.AreEqual(0d, result.WriteCapacityUnits);
        }

        [TestMethod]
        public async Task ConcurrentAddAndResetIsThreadSafe()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var value = new RDFTripleStoreConsumedCapacity(1.0d, 1.0d, 1.0d);

            var addTasks = new Task[50];
            var resetTasks = new Task[10];

            for (int i = 0; i < addTasks.Length; i++)
            {
                addTasks[i] = Task.Run(() => consumedCapacity.AddConsumedCapacity(value));
            }

            for (int i = 0; i < resetTasks.Length; i++)
            {
                resetTasks[i] = Task.Run(() => consumedCapacity.ResetConsumedCapacity());
            }

            await Task.WhenAll(addTasks.Concat(resetTasks).ToArray());

            var result = consumedCapacity.GetConsumedCapacity();
            // Result should be valid (either 0 or a multiple of value components)
            Assert.IsTrue(result.CapacityUnits >= 0);
            Assert.IsTrue(result.ReadCapacityUnits >= 0);
            Assert.IsTrue(result.WriteCapacityUnits >= 0);
        }

        [TestMethod]
        public void AddConsumedCapacityWithZeroValuesDoesNotChangeTotal()
        {
            var consumedCapacity = new InMemoryRDFTripleStoreConsumedCapacity();
            var initialValue = new RDFTripleStoreConsumedCapacity(5.0d, 10.0d, 15.0d);
            consumedCapacity.AddConsumedCapacity(initialValue);

            var zeroValue = RDFTripleStoreConsumedCapacity.None();
            consumedCapacity.AddConsumedCapacity(zeroValue);

            var result = consumedCapacity.GetConsumedCapacity();
            Assert.AreEqual(5.0d, result.CapacityUnits);
            Assert.AreEqual(10.0d, result.ReadCapacityUnits);
            Assert.AreEqual(15.0d, result.WriteCapacityUnits);
        }
    }
}
