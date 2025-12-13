/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public sealed record RDFTripleStoreOptions
    {
        public bool ScopeCacheEnabled { get; set; }

        public bool BatchingEnabled { get; set; }

        public bool TrackConsumedCapacity { get; set; }
    }
}
