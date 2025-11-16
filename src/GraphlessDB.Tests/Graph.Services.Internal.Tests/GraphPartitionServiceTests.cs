/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class GraphPartitionServiceTests
    {
        [TestMethod]
        public void CanGetCorrectPartition()
        {
            var options = Options.Create(new GraphOptions { PartitionCount = 10 });
            var graphSettingsService = new GraphDBSettingsService(options);
            var partitionMapper = new GraphPartitionService(graphSettingsService);
            var partition0 = partitionMapper.GetPartition("Q29udGFjdCMwMzE2NzliNS00YzkzLTQxYmEtYmUwZS1iMDRhMTJlMTQwNWE=");
            Assert.AreEqual("2", partition0);
            var partition1 = partitionMapper.GetPartition("a");
            Assert.AreEqual("7", partition1);
            var partition2 = partitionMapper.GetPartition("b");
            Assert.AreEqual("4", partition2);
            var partition3 = partitionMapper.GetPartition("c");
            Assert.AreEqual("5", partition3);
            var partition4 = partitionMapper.GetPartition("d");
            Assert.AreEqual("0", partition4);
            var partition5 = partitionMapper.GetPartition("e");
            Assert.AreEqual("1", partition5);
            var partition6 = partitionMapper.GetPartition("f");
            Assert.AreEqual("8", partition6);
            var partition7 = partitionMapper.GetPartition("g");
            Assert.AreEqual("9", partition7);
            var partition8 = partitionMapper.GetPartition("h");
            Assert.AreEqual("8", partition8);
            var partition9 = partitionMapper.GetPartition("i");
            Assert.AreEqual("9", partition9);
            var partition10 = partitionMapper.GetPartition("j");
            Assert.AreEqual("6", partition10);
        }
    }
}
