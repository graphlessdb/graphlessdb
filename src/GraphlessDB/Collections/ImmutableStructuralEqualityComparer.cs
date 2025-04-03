/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections;
using System.Collections.Immutable;
using System.Linq;

namespace GraphlessDB.Collections
{
    public sealed class ImmutableStructuralEqualityComparer : IEqualityComparer
    {
        public static readonly ImmutableStructuralEqualityComparer Default = new();

        public new bool Equals(object? x, object? y)
        {
            return x switch
            {
                IDictionary dic => Equals(dic, (IDictionary?)y),
                IList list => Equals(list, (IList?)y),
                _ => StructuralComparisons.StructuralEqualityComparer.Equals(x, y),
            };
        }

        public int GetHashCode(object obj)
        {
            return obj switch
            {
                IDictionary dic => GetHashCode(dic),
                IList list => GetHashCode(list),
                _ => StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj),
            };
        }

        private bool Equals(IList? x, IList? y)
        {
            if (x == null && y == null)
            {
                return true;
            }

            if (x == null || y == null)
            {
                return false;
            }

            if (x.Count != y.Count)
            {
                return false;
            }

            for (var i = 0; i < x.Count; i++)
            {
                if (!Equals(x[i], y[i]))
                {
                    return false;
                }
            }

            return true;
        }

        private bool Equals(IDictionary? x, IDictionary? y)
        {
            if (x == null && y == null)
            {
                return true;
            }

            if (x == null || y == null)
            {
                return false;
            }

            var xKeys = x.Keys.Cast<object>().OrderBy(v => v).ToImmutableArray();
            var yKeys = y.Keys.Cast<object>().OrderBy(v => v).ToImmutableArray();
            if (!Equals(xKeys, yKeys))
            {
                return false;
            }

            var xValues = xKeys.Select(k => x[k]).ToImmutableArray();
            var yValues = yKeys.Select(k => y[k]).ToImmutableArray();
            if (!Equals(xKeys, yKeys))
            {
                return false;
            }

            return true;
        }

        private int GetHashCode(IList obj)
        {
            return obj.Cast<object>().Aggregate(0, (acc, cur) =>
            {
                if (cur != null)
                {
                    acc ^= GetHashCode(cur);
                }

                return acc;
            });
        }

        private int GetHashCode(IDictionary obj)
        {
            return obj.Keys.Cast<object>().OrderBy(v => v).Aggregate(0, (acc, cur) =>
            {
                acc ^= GetHashCode(cur);
                if (obj.Contains(cur))
                {
                    var value = obj[cur];
                    if (value != null)
                    {
                        acc ^= GetHashCode(value);
                    }
                }

                return acc;
            });
        }
    }
}
