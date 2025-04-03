/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public sealed record UpdateRDFTripleAllEdgesVersion(
        string TableName,
        RDFTripleKey Key,
        VersionDetailCondition VersionDetailCondition,
        int AllEdgesVersion);
}
