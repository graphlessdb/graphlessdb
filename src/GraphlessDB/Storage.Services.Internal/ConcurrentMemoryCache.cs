/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Concurrent;

namespace GraphlessDB.Storage.Services.Internal
{
    internal sealed class ConcurrentMemoryCache : IMemoryCache
    {
        private readonly ConcurrentDictionary<object, object> _cache;

        public ConcurrentMemoryCache()
        {
            _cache = new ConcurrentDictionary<object, object>();
        }

        public bool TryGetValue(object key, out object? value)
        {
            return _cache.TryGetValue(key, out value);
        }

        public bool TryRemove(object key, out object? value)
        {
            return _cache.TryRemove(key, out value);
        }

        public void AddOrUpdate(object key, object value)
        {
            _cache.AddOrUpdate(key, _ => value, (_, _) => value);
        }
    }
}
