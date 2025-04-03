/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Graph.Services.Internal
{
    internal interface IGraphEntitySerializationService
    {
        string SerializeNode(INode node, Type type);

        string SerializeEdge(IEdge edge, Type type);

        INode DeserializeNode(string value, Type type);

        IEdge DeserializeEdge(string value, Type type);
    }
}
