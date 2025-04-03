/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed class RelayEdge<T>(string cursor, T node) : IRelayEdge<T>
    {
        public string Cursor { get; } = cursor;

        public T Node { get; } = node;
    }
}