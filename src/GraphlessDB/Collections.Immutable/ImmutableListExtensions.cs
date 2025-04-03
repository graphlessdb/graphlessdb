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
    public static class ImmutableListExtensions
    {
        public static ImmutableListSequence<T> ToImmutableListSequence<T>(this ImmutableList<T> source)
        {
            return new ImmutableListSequence<T>(source);
        }

        public static ImmutableList<T> AddIf<T>(this ImmutableList<T> source, bool condition, T value)
        {
            return condition ? source.Add(value) : source;
        }
    }
}
