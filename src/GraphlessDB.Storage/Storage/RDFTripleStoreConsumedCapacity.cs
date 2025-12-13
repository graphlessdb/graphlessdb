/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public sealed record RDFTripleStoreConsumedCapacity(double CapacityUnits, double ReadCapacityUnits, double WriteCapacityUnits)
    {
        // public static readonly RDFTripleStoreConsumedCapacity None = new(0d, 0d, 0d);
        public static RDFTripleStoreConsumedCapacity None()
        {
            return new RDFTripleStoreConsumedCapacity(0d, 0d, 0d);
        }
    }
}
