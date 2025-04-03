/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Storage
{
    public sealed record RDFTripleKeyWithPartition(
        string Subject, string Predicate, string Partition) : IComparable<RDFTripleKeyWithPartition>
    {
        public int CompareTo(RDFTripleKeyWithPartition? other)
        {
            return ((string?)Subject, (string?)Predicate, (string?)Partition)
                .CompareTo((other?.Subject, other?.Predicate, other?.Partition));
        }

        public static bool operator <(RDFTripleKeyWithPartition left, RDFTripleKeyWithPartition right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator >(RDFTripleKeyWithPartition left, RDFTripleKeyWithPartition right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator <=(RDFTripleKeyWithPartition left, RDFTripleKeyWithPartition right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >=(RDFTripleKeyWithPartition left, RDFTripleKeyWithPartition right)
        {
            return left.CompareTo(right) >= 0;
        }
    }
}
