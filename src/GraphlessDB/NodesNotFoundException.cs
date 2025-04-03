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
    public class NodesNotFoundException(ImmutableList<string> nodeIds) : Exception($"Nodes [{string.Join(",", nodeIds)}] could not be found")
    {
        public NodesNotFoundException()
            : this([])
        {
        }

        public NodesNotFoundException(string nodeKey)
            : this([nodeKey])
        {
        }

        public ImmutableList<string> NodeIds { get; } = nodeIds;
    }
}
