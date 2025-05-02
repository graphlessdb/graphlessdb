/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class GraphEntityTypeNativeService(IOptionsSnapshot<GraphEntityTypeNativeServiceOptions> options) : IGraphEntityTypeService
    {
        public Type GetEntityType(string typeName)
        {
            return options.Value.TypeMappings[typeName];
        }
    }
}
