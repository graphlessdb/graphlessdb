/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage.Services
{
    public interface IMemoryCache
    {
        bool TryGetValue(object key, out object? value);

        bool TryRemove(object key, out object? value);

        void AddOrUpdate(object key, object value);
    }
}
