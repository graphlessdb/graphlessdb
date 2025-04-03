/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using GraphlessDB.Storage;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class GraphEntitySerializationService(IOptions<GraphEntitySerializationServiceOptions> options) : IGraphEntitySerializationService
    {
        public IEdge DeserializeEdge(string value, Type type)
        {
            return (IEdge?)JsonSerializer.Deserialize(value, type, GetJsonContext()) ?? throw new GraphlessDBOperationException("Failed to deserialize edge");
        }

        public INode DeserializeNode(string value, Type type)
        {
            return (INode?)JsonSerializer.Deserialize(value, type, GetJsonContext()) ?? throw new GraphlessDBOperationException("Failed to deserialize node");
        }

        public string SerializeNode(INode node, Type type)
        {
            return JsonSerializer.Serialize(node, type, GetJsonContext());
        }

        public string SerializeEdge(IEdge edge, Type type)
        {
            return JsonSerializer.Serialize(edge, type, GetJsonContext());
        }

        private JsonSerializerContext GetJsonContext()
        {
            return options.Value.JsonContext ?? throw new GraphlessDBOperationException("EntitySerializerOptions.JsonContext was not set");
        }
    }
}
