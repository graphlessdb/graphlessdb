/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage.Services.Internal.FileBased
{
    internal sealed class FileBasedRDFTripleStoreOptions
    {
        public FileBasedRDFTripleStoreOptions()
        {
            StoragePath = string.Empty;
        }

        public string StoragePath { get; set; }
    }
}
