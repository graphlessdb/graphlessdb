/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class GraphEntityTypeService(IOptionsSnapshot<GraphDBAssemblyTypeMapperOptions> options) : IGraphEntityTypeService
    {
        [UnconditionalSuppressMessage("DynamicallyAccessedMembersAttribute", "IL2075")]
        [UnconditionalSuppressMessage("RequiresUnreferencedCode", "IL2057")]
        public Type GetEntityType(string typeName)
        {
            var fullTypeName = $"{string.Join(".", options.Value.Namespace, typeName)}, {options.Value.AssemblyName}";
            var type = Type.GetType(fullTypeName);
            return type ?? throw new GraphlessDBOperationException($"The type name '{typeName}' was expanded to '{fullTypeName}' but not valid types was found");
        }
    }
}