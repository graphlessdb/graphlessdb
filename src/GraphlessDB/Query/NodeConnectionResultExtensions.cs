/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Query
{
    public static class NodeConnectionResultExtensions
    {
        public static NodeConnectionResult EnsureValid(this NodeConnectionResult source)
        {
            if (source.ChildCursor == string.Empty)
            {
                throw new GraphlessDBOperationException("Empty ChildCursor was not expected");
            }

            if (source.Cursor == string.Empty)
            {
                throw new GraphlessDBOperationException("Empty Cursor was not expected");
            }

            return source;
        }
    }
}
