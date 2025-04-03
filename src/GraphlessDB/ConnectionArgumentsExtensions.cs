/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public static class ConnectionArgumentsExtensions
    {
        public static bool HasCursor(this ConnectionArguments source)
        {
            return source.CursorOrDefault() != null;
        }

        public static string? CursorOrDefault(this ConnectionArguments source)
        {
            var value = source.First.HasValue ? source.After : source.Before;
            if (value == string.Empty)
            {
                throw new GraphlessDBOperationException("Empty cursor string not expected");
            }

            return value;
        }

        public static string Cursor(this ConnectionArguments source)
        {
            return source.CursorOrDefault() ?? throw new GraphlessDBOperationException("Cursor is missing");
        }

        public static int Count(this ConnectionArguments source)
        {
            return source.First ?? source.Last ?? throw new GraphlessDBOperationException("Count is missing");
        }

        public static ConnectionArguments WithCursor(this ConnectionArguments source, string? value)
        {
            return source.First.HasValue
                ? new ConnectionArguments(source.First, value, null, null)
                : new ConnectionArguments(null, null, source.Last, value);
        }


        public static ConnectionArguments WithCount(this ConnectionArguments source, int count)
        {
            return source.First.HasValue
                ? new ConnectionArguments(count, source.After, null, null)
                : new ConnectionArguments(null, null, count, source.Before);
        }
    }
}
