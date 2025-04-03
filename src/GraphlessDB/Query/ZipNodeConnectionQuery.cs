/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Collections;

namespace GraphlessDB.Query
{
    public record ZipNodeConnectionQuery(
        ImmutableTree<string, GraphQueryNode> Other,
        ConnectionArguments Page,
        int PreFilteredPageSize,
        string? Tag) : GraphQuery;
}
