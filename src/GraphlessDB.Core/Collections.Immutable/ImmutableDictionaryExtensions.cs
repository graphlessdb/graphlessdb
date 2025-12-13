/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.Collections.Immutable
{
    public static class ImmutableDictionaryExtensions
    {
        public static ImmutableDictionarySequence<TKey, TValue> ToImmutableDictionarySequence<TKey, TValue>(this ImmutableDictionary<TKey, TValue> source)
            where TKey : notnull
        {
            return new ImmutableDictionarySequence<TKey, TValue>(source);
        }
    }
}
