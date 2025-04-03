/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphlessDB.Collections.Immutable
{
    public static class ImmutableListSequenceExtensions
    {
        public static ImmutableListSequence<T> Add<T>(ImmutableListSequence<T> source, T item)
        {
            return source.Items.Add(item).ToImmutableListSequence();
        }

        public static ImmutableListSequence<T> Add<T>(ImmutableListSequence<T> source, IEnumerable<T> items)
        {
            return source.Items.AddRange(items).ToImmutableListSequence();
        }

        public static ImmutableListSequence<T> SetItem<T>(ImmutableListSequence<T> source, int index, T value)
        {
            return source.Items.SetItem(index, value).ToImmutableListSequence();
        }

        public static ImmutableListSequence<T> ReplaceSingle<T>(ImmutableListSequence<T> source, Func<T, bool> selector, Func<T, T> updater)
        {
            var item = source.Items.Where(selector).Single();
            var updatedItem = updater(item);
            return source.Items.Replace(item, updatedItem).ToImmutableListSequence();
        }

        public static int IndexOf<T>(ImmutableListSequence<T> source, T item)
        {
            return source.Items.IndexOf(item);
        }
    }
}
