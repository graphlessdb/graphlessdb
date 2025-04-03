/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB
{
    public sealed record EdgeFilter(
        string EdgeTypeName,
        string NodeInTypeName,
        string NodeOutTypeName,
        INodeFilter? NodeInFilter,
        INodeFilter? NodeOutFilter,
        ImmutableList<ValueFilterItem> ValueFilterItems);
}
