/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;

namespace GraphlessDB.Collections
{
    public static class ImmutableNodeList
    {
        public static ImmutableNodeList<TNode, TKey> Create<TNode, TKey>(ImmutableList<TNode> nodes, Func<TNode, TKey> nodeKeySelector) where TKey : notnull
        {
            return new ImmutableNodeList<TNode, TKey>(nodes.ToImmutableDictionary(nodeKeySelector));
        }
    }
}
