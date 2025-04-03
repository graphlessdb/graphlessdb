/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Graph.Services.Internal
{
    internal class GraphPartitionService(IGraphSettingsService optionsProvider) : IGraphPartitionService
    {
        public string GetPartition(string id)
        {
            var hashcode = GetDeterministicHashCode(id);
            var partitionCount = optionsProvider.GetGraphSettings().PartitionCount;
            var value = hashcode % partitionCount;
            var absValue = Math.Abs(value);
            var stringValue = $"{absValue}";
            return stringValue;
        }

        private static int GetDeterministicHashCode(string value)
        {
            unchecked
            {
                var hash1 = (5381 << 16) + 5381;
                var hash2 = hash1;
                for (var i = 0; i < value.Length; i += 2)
                {
                    hash1 = (hash1 << 5) + hash1 ^ value[i];
                    if (i == value.Length - 1)
                    {
                        break;
                    }

                    hash2 = (hash2 << 5) + hash2 ^ value[i + 1];
                }

                return hash1 + hash2 * 1566083941;
            }
        }
    }
}