/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.Query
{
    public sealed record NodeFilter(
        ImmutableList<ValueFilterItem> ValueFilterItems,
        ImmutableList<EdgeFilter> EdgeFilterItems)
    {
        public static readonly NodeFilter Empty = new([], []);
    }
}
