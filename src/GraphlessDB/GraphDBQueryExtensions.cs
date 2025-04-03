/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Collections;

namespace GraphlessDB
{
    public static class GraphDBQueryExtensions
    {
        public static FluentGraphQuery<DefaultGraph> Graph(this IGraphDB source)
        {
            return new FluentGraphQuery<DefaultGraph>(source.QueryExecutionService, ImmutableTree<string, Query.GraphQueryNode>.Empty, string.Empty);
        }

        public static FluentGraphQuery<TGraph> Graph<TGraph>(this IGraphDB source) where TGraph : IGraph
        {
            return new FluentGraphQuery<TGraph>(source.QueryExecutionService, ImmutableTree<string, Query.GraphQueryNode>.Empty, string.Empty);
        }
    }
}