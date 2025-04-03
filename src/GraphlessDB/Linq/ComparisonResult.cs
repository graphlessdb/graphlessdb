/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;

namespace GraphlessDB.Linq
{
    public sealed class ComparisonResult<TKey, TValue>(
        ImmutableDictionary<TKey, TValue> match,
        ImmutableDictionary<TKey, Tuple<TValue, TValue>> different,
        ImmutableDictionary<TKey, TValue> onlyIn1,
        ImmutableDictionary<TKey, TValue> onlyIn2) where TKey : notnull
    {
        public ImmutableDictionary<TKey, TValue> Match { get; } = match;

        public ImmutableDictionary<TKey, Tuple<TValue, TValue>> Different { get; } = different;

        public ImmutableDictionary<TKey, TValue> OnlyIn1 { get; } = onlyIn1;

        public ImmutableDictionary<TKey, TValue> OnlyIn2 { get; } = onlyIn2;
    }
}