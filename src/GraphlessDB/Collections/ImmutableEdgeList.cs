/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;

namespace GraphlessDB.Collections
{
    public static class ImmutableEdgeList
    {
        internal static ImmutableEdgeList<TEdge, TKey> Create<TEdge, TKey>(ImmutableList<TEdge> edges, Func<TEdge, TKey> edgeInKeySelector, Func<TEdge, TKey> edgeOutKeySelector) where TKey : notnull
        {
            return new ImmutableEdgeList<TEdge, TKey>(
                edges.GroupBy(k => edgeInKeySelector(k)).ToImmutableDictionary(k => k.Key, v => v.ToImmutableList()),
                edges.GroupBy(k => edgeOutKeySelector(k)).ToImmutableDictionary(k => k.Key, v => v.ToImmutableList()));
        }
    }
}