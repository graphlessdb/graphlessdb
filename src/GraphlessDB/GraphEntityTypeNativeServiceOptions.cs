

using System;
using System.Collections.Generic;


/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */
namespace GraphlessDB
{
    public class GraphEntityTypeNativeServiceOptions
    {
        public GraphEntityTypeNativeServiceOptions()
        {
            TypeMappings = new Dictionary<string, Type>();
        }

        public Dictionary<string, Type> TypeMappings { get; }
    }
}
