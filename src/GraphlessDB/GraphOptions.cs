/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed class GraphOptions
    {
        public GraphOptions()
        {
            TableName = string.Empty;
            GraphName = string.Empty;
            PartitionCount = 1;
        }

        public string TableName { get; set; }

        public string GraphName { get; set; }

        public int PartitionCount { get; set; }

        public string ByPredicateIndexName => $"{TableName}ByPredicate";

        public string ByObjectIndexName => $"{TableName}ByObject";
    }
}
