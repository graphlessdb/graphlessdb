/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB
{
    public sealed class MutationId(string clientMutationId)
    {
        private int _index = -1;
        private readonly string _clientMutationId = clientMutationId;

        public static MutationId Create(string clientMutationId)
        {
            return new MutationId(clientMutationId);
        }

        public string Next()
        {
            _index += 1;

            // DynamoDB can only take idepotency keys up to 36 chars
            var key = $"{_index}-{_clientMutationId}";
            return key.Length > 36 ? key[..36] : key;
        }
    }
}