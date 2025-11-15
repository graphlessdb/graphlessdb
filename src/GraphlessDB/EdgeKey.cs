/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Diagnostics.CodeAnalysis;

namespace GraphlessDB
{
    public sealed class EdgeKey(string typeName, string inId, string outId) : IEquatable<EdgeKey>, IComparable<EdgeKey>
    {
        public string TypeName { get; } = typeName;

        public string InId { get; } = inId;

        public string OutId { get; } = outId;

        public int CompareTo([AllowNull] EdgeKey other)
        {
            if (other == null)
            {
                return -1;
            }

            var comparison = string.CompareOrdinal(TypeName, other.TypeName);
            if (comparison != 0)
            {
                return comparison;
            }

            comparison = string.CompareOrdinal(InId, other.InId);
            if (comparison != 0)
            {
                return comparison;
            }

            return string.CompareOrdinal(OutId, other.OutId);
        }

        public override bool Equals(object? obj)
        {
            return obj is EdgeKey key &&
                   TypeName == key.TypeName &&
                   InId == key.InId &&
                   OutId == key.OutId;
        }

        public bool Equals([AllowNull] EdgeKey other)
        {
            return Equals((object?)other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(TypeName, InId, OutId);
        }

        public override string ToString()
        {
            return $"{TypeName}#{InId}#{OutId}";
        }

        public static bool operator ==([AllowNull] EdgeKey left, [AllowNull] EdgeKey right)
        {
            if (left is null && right is null)
            {
                return true;
            }

            if (left is null || right is null)
            {
                return false;
            }

            return left.CompareTo(right) == 0;
        }

        public static bool operator !=([AllowNull] EdgeKey left, [AllowNull] EdgeKey right)
        {
            if (left is null && right is null)
            {
                return false;
            }

            if (left is null || right is null)
            {
                return true;
            }

            return left.CompareTo(right) != 0;
        }

        public static bool operator <(EdgeKey left, EdgeKey right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator >(EdgeKey left, EdgeKey right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator <=(EdgeKey left, EdgeKey right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >=(EdgeKey left, EdgeKey right)
        {
            return left.CompareTo(right) >= 0;
        }
    }
}
