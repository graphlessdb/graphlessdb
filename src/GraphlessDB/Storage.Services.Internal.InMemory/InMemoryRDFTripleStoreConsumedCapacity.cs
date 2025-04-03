/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal sealed class InMemoryRDFTripleStoreConsumedCapacity : IRDFTripleStoreConsumedCapacity
    {
        private RDFTripleStoreConsumedCapacity _total;
        private readonly Lock _locker;

        public InMemoryRDFTripleStoreConsumedCapacity()
        {
            _total = RDFTripleStoreConsumedCapacity.None();
            _locker = new Lock();
        }

        public void AddConsumedCapacity(RDFTripleStoreConsumedCapacity value)
        {
            lock (_locker)
            {
                _total = Add(_total, value);
            }
        }

        private static RDFTripleStoreConsumedCapacity Add(
            RDFTripleStoreConsumedCapacity total, RDFTripleStoreConsumedCapacity value)
        {
            return new RDFTripleStoreConsumedCapacity(
                total.CapacityUnits + value.CapacityUnits,
                total.ReadCapacityUnits + value.ReadCapacityUnits,
                total.WriteCapacityUnits + value.WriteCapacityUnits);
        }

        public RDFTripleStoreConsumedCapacity GetConsumedCapacity()
        {
            return _total;
        }

        public void ResetConsumedCapacity()
        {
            lock (_locker)
            {
                _total = RDFTripleStoreConsumedCapacity.None();
            }
        }
    }
}
