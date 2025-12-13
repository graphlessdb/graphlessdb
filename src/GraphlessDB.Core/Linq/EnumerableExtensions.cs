/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace GraphlessDB.Linq
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T?> source) where T : class
        {
            return source
                .Where(a => a != null)
                .Select(a => a!);
        }

        public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T?> source) where T : struct
        {
            return source
                .Where(a => a.HasValue)
                .Select(a => a!.Value);
        }

        public static IEnumerable<T> NotNull<T>(this IEnumerable<T?> source) where T : class
        {
            return source
                .Select(a => a != null ? a! : throw new InvalidOperationException());
        }

        public static IEnumerable<T> NotNull<T>(this IEnumerable<T?> source) where T : struct
        {
            return source
                .Select(a => a.HasValue ? a!.Value : throw new InvalidOperationException());
        }

        public static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(this ImmutableDictionary<TKey, TValue> source) where TKey : notnull
        {
            return source.ToDictionary(k => k.Key, v => v.Value);
        }

        public static IOrderedEnumerable<TSource> OrderByDirection<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector, bool descending, IComparer<TKey>? comparer = null)
        {
            return descending
                ? source.OrderByDescending(keySelector, comparer)
                : source.OrderBy(keySelector, comparer);
        }

        public static IEnumerable<List<T>> ToListBatches<T>(this IEnumerable<T> source, int batchSize)
        {
            var batch = ImmutableList.CreateBuilder<T>();
            foreach (var item in source)
            {
                batch.Add(item);

                if (batch.Count >= batchSize)
                {
                    yield return batch.ToList();

                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                yield return batch.ToList();
            }
        }

        public static IEnumerable<ImmutableList<T>> ToImmutableListBatches<T>(this IEnumerable<T> source, int batchSize)
        {
            var batch = ImmutableList.CreateBuilder<T>();
            foreach (var item in source)
            {
                batch.Add(item);

                if (batch.Count >= batchSize)
                {
                    yield return batch.ToImmutableList();

                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                yield return batch.ToImmutableList();
            }
        }

        public static ComparisonResultSet<T> CompareTo<T>(this ImmutableHashSet<T> source, ImmutableHashSet<T> other)
        {
            var match = source
                .Where(other.Contains)
                .ToImmutableList();

            var onlyIn1 = source
                .Where(kv => !other.Contains(kv))
                .ToImmutableList();

            var onlyIn2 = other
                .Where(kv => !source.Contains(kv))
                .ToImmutableList();

            return new ComparisonResultSet<T>(
                match,
                onlyIn1,
                onlyIn2);
        }

        public static ComparisonResult<TKey, TValueCompared> CompareTo<TKey, TValue1, TValue2, TValueCompared>(
            this ImmutableDictionary<TKey, TValue1> source,
            ImmutableDictionary<TKey, TValue2> other,
            Func<TValue1, TValueCompared> value1Selector,
            Func<TValue2, TValueCompared> value2Selector)
            where TKey : notnull
        {
            var match = source
                .Where(kv => other.ContainsKey(kv.Key) && Equals(value1Selector(kv.Value), value2Selector(other[kv.Key])))
                .ToImmutableDictionary(
                    k => k.Key,
                    v => value1Selector(v.Value));

            var different = source
                .Where(kv => other.ContainsKey(kv.Key) && !Equals(value1Selector(kv.Value), value2Selector(other[kv.Key])))
                .ToImmutableDictionary(
                    k => k.Key,
                    v => new Tuple<TValueCompared, TValueCompared>(value1Selector(v.Value), value2Selector(other[v.Key])));

            var onlyIn1 = source
                .Where(kv => !other.ContainsKey(kv.Key))
                .ToImmutableDictionary(
                    k => k.Key,
                    v => value1Selector(v.Value));

            var onlyIn2 = other
                .Where(kv => !source.ContainsKey(kv.Key))
                .ToImmutableDictionary(
                    k => k.Key,
                    v => value2Selector(v.Value));

            return new ComparisonResult<TKey, TValueCompared>(
                match,
                different,
                onlyIn1,
                onlyIn2);
        }
    }
}