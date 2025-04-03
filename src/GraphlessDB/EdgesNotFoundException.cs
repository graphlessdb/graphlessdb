/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;

namespace GraphlessDB
{
    // TODO This needs to be renamed, it's used in a case where database may by updated between composite queries
    public class EdgesNotFoundException(ImmutableList<EdgeKey> edgeKeys, string? message = null, Exception? innerException = null) : Exception(message, innerException)
    {
        public EdgesNotFoundException() : this([])
        {
        }

        public ImmutableList<EdgeKey> EdgeKeys { get; } = edgeKeys;
    }
}
