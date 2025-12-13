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
    public sealed record RDFTripleKey(string Subject, string Predicate) : IComparable<RDFTripleKey>
    {
        public int CompareTo(RDFTripleKey? other)
        {
            return ((string?)Subject, (string?)Predicate)
                .CompareTo((other?.Subject, other?.Predicate));
        }

        public static bool operator <(RDFTripleKey left, RDFTripleKey right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator >(RDFTripleKey left, RDFTripleKey right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator <=(RDFTripleKey left, RDFTripleKey right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >=(RDFTripleKey left, RDFTripleKey right)
        {
            return left.CompareTo(right) >= 0;
        }
    }
}
