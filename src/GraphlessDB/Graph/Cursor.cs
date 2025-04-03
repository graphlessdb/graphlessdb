/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Collections;

namespace GraphlessDB.Graph
{
    public sealed record Cursor(ImmutableTree<string, CursorNode> Items)
    {
        public static Cursor Create(CursorNode root)
        {
            return new Cursor(ImmutableTree<string, CursorNode>.Empty.AddNode("[0]", root));
        }
    }
}
