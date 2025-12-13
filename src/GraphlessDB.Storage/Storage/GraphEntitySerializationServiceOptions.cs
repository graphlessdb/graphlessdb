/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace GraphlessDB.Storage
{
    public sealed class GraphEntitySerializationServiceOptions
    {
        public GraphEntitySerializationServiceOptions()
        {
            JsonContextOverrides = new Dictionary<Type, JsonSerializerContext>();
        }

        public JsonSerializerContext? JsonContext { get; set; }

        public Dictionary<Type, JsonSerializerContext> JsonContextOverrides { get; set; }
    }
}
