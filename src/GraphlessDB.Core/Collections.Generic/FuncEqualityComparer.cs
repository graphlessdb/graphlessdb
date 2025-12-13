/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */


using System;
using System.Collections.Generic;

namespace GraphlessDB.Collections.Generic
{
    public sealed class FuncEqualityComparer<T>(Func<T?, T?, bool> comparer, Func<T, int> hash) : IEqualityComparer<T>
    {
        public FuncEqualityComparer(Func<T?, T?, bool> comparer)
            : this(comparer, t => 0)
        {
        }

        public bool Equals(T? x, T? y)
        {
            return comparer(x, y);
        }

        public int GetHashCode(T obj)
        {
            return hash(obj);
        }
    }
}