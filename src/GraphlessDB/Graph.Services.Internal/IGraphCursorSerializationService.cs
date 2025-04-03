/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Graph.Services.Internal
{
    internal interface IGraphCursorSerializationService
    {
        string Serialize(Cursor value);

        Cursor Deserialize(string value);
    }
}
