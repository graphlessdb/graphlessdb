/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB
{
    public sealed class ConnectionArguments
    {
        public static readonly ConnectionArguments Default = new(25);
        public static readonly ConnectionArguments FirstOne = new(1);
        public static readonly ConnectionArguments FirstMax = new(int.MaxValue);

        public ConnectionArguments(
            int? first = default,
            string? after = default,
            int? last = default,
            string? before = default)
        {
            if (first != null && last != null)
            {
                throw new ArgumentException("First and Last cannot both be specified together");
            }

            if (first == null && last == null)
            {
                throw new ArgumentException("First or Last must be specified");
            }

            if (after != null && before != null)
            {
                throw new ArgumentException("After and Before cannot both be specified together");
            }

            First = first;
            After = after;
            Last = last;
            Before = before;
        }

        public int? First { get; }

        public string? After { get; }

        public int? Last { get; }

        public string? Before { get; }

        public int Count()
        {
            return First ?? Last ?? throw new GraphlessDBOperationException("First or Last must be specified");
        }

        public static ConnectionArguments GetFirst(int value, string? after = null)
        {
            return new ConnectionArguments(value, after);
        }
        public static ConnectionArguments GetLast(int value, string? before = null)
        {
            return new ConnectionArguments(null, null, value, before);
        }
    }
}