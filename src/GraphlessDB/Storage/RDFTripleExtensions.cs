/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Globalization;

namespace GraphlessDB.Storage
{
    public static class RDFTripleExtensions
    {
        public static RDFTripleKey AsKey(this RDFTriple source)
        {
            return new RDFTripleKey(source.Subject, source.Predicate);
        }

        public static int GetPartition(this RDFTriple source)
        {
            return int.Parse(source.Partition, CultureInfo.InvariantCulture);
        }
    }
}
