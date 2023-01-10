using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Newtonsoft.Json;
using Nop.Core.Configuration;
using Nop.Core.Infrastructure;

namespace Nop.Core.Caching
{
    /// <summary>
    /// A distributed cache manager that locks the acquisition task
    /// </summary>
    public abstract class DistributedCacheManager : CacheKeyService, IStaticCacheManager
    {
        #region Fields

        private static readonly ConcurrentTrie<Lazy<CacheLock>> _locksByKey = new();
        private readonly IDistributedCache _distributedCache;
        private readonly ConcurrentTrie<object> _perRequestCache = new();

        #endregion

        #region Ctor

        public DistributedCacheManager(AppSettings appSettings, IDistributedCache distributedCache) : base(appSettings)
        {
            _distributedCache = distributedCache;
        }

        #endregion

        #region Utilities

        /// <summary>
        /// Clear all data on this instance
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        protected void ClearInstanceData()
        {
            _perRequestCache.Clear();
            _locksByKey.Clear();
        }

        /// <summary>
        /// Remove items by cache key prefix
        /// </summary>
        /// <param name="prefix">Cache key prefix</param>
        /// <param name="prefixParameters">Parameters to create cache key prefix</param>
        /// <returns>The removed keys</returns>
        protected IEnumerable<string> RemoveByPrefixInstanceData(string prefix, params object[] prefixParameters)
        {
            var prefix_ = PrepareKeyPrefix(prefix, prefixParameters);
            _perRequestCache.Prune(prefix_, out _);
            _locksByKey.Prune(prefix_, out var subtree);
            return subtree.Keys;
        }

        private static async Task<CacheLock> AcquireLockAsync(string key)
        {
            while (true)
            {
                var cacheLock = _locksByKey.GetOrAdd(key, () => new Lazy<CacheLock>(() => new(), true)).Value;
                try
                {
                    await cacheLock.WaitAsync();
                    return cacheLock;
                }
                catch   // cacheLock was removed while waiting, acquire a new instance
                {
                }
            }
        }

        /// <summary>
        /// Prepare cache entry options for the passed key
        /// </summary>
        /// <param name="key">Cache key</param>
        /// <returns>Cache entry options</returns>
        private static DistributedCacheEntryOptions PrepareEntryOptions(CacheKey key)
        {
            //set expiration time for the passed cache key
            return new DistributedCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(key.CacheTime)
            };
        }

        private async Task<(bool isSet, T item)> TryGetItemAsync<T>(string key)
        {
            var cacheLock = await AcquireLockAsync(key);
            try
            {
                var json = await _distributedCache.GetStringAsync(key);

                if (string.IsNullOrEmpty(json))
                    return (false, default);

                var item = JsonConvert.DeserializeObject<T>(json);
                _perRequestCache.Set(key, item);

                return (true, item);
            }
            finally
            {
                cacheLock.Release();
            }
        }

        private async Task<T> GetOrSetAsync<T>(CacheKey key, Func<Task<T>> acquire, bool forceOverwrite)
        {
            if ((key?.CacheTime ?? 0) <= 0)
                return await acquire();

            var setTask = Task.CompletedTask;
            var cacheLock = await AcquireLockAsync(key.Key);
            try
            {
                T data = default;
                if (!forceOverwrite)
                {
                    if (_perRequestCache.TryGetValue(key.Key, out var value))
                        return (T)value;
                    var json = await _distributedCache.GetStringAsync(key.Key);
                    if (!string.IsNullOrEmpty(json))
                    {
                        data = JsonConvert.DeserializeObject<T>(json);
                        _perRequestCache.Set(key.Key, data);
                        return data;
                    }
                }
                data = await acquire();
                if (data != null)
                {
                    _perRequestCache.Set(key.Key, data);
                    setTask = _distributedCache.SetStringAsync(key.Key, JsonConvert.SerializeObject(data), PrepareEntryOptions(key));
                }
                return data;
            }
            finally
            {
                _ = setTask.ContinueWith(_ => cacheLock.Release());
            }
        }

        private async Task RemoveAsync(string key, bool removeFromPerRequestCache = true)
        {
            var cacheLock = await AcquireLockAsync(key);
            await _distributedCache.RemoveAsync(key);
            if (removeFromPerRequestCache)
                _perRequestCache.TryRemove(key);
            _locksByKey.TryRemove(key);
            cacheLock.Cancel();
        }

        #endregion

        #region Methods

        /// <summary>
        /// Remove the value with the specified key from the cache
        /// </summary>
        /// <param name="cacheKey">Cache key</param>
        /// <param name="cacheKeyParameters">Parameters to create cache key</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public async Task RemoveAsync(CacheKey cacheKey, params object[] cacheKeyParameters)
        {
            await RemoveAsync(PrepareKey(cacheKey, cacheKeyParameters).Key);
        }

        /// <summary>
        /// Get a cached item. If it's not in the cache yet, then load and cache it
        /// </summary>
        /// <typeparam name="T">Type of cached item</typeparam>
        /// <param name="key">Cache key</param>
        /// <param name="acquire">Function to load item if it's not in the cache yet</param>
        /// <returns>
        /// A task that represents the asynchronous operation
        /// The task result contains the cached value associated with the specified key
        /// </returns>
        public async Task<T> GetAsync<T>(CacheKey key, Func<Task<T>> acquire)
        {
            if (_perRequestCache.TryGetValue(key.Key, out var data))
                return (T)data;
            var (isSet, item) = await TryGetItemAsync<T>(key.Key);
            return isSet ? item : await GetOrSetAsync(key, acquire, false);
        }

        /// <summary>
        /// Get a cached item. If it's not in the cache yet, then load and cache it
        /// </summary>
        /// <typeparam name="T">Type of cached item</typeparam>
        /// <param name="key">Cache key</param>
        /// <param name="acquire">Function to load item if it's not in the cache yet</param>
        /// <returns>
        /// A task that represents the asynchronous operation
        /// The task result contains the cached value associated with the specified key
        /// </returns>
        public Task<T> GetAsync<T>(CacheKey key, Func<T> acquire)
        {
            return GetAsync(key, () => Task.FromResult(acquire()));
        }

        /// <summary>
        /// Add the specified key and object to the cache
        /// </summary>
        /// <param name="key">Key of cached item</param>
        /// <param name="data">Value for caching</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public Task SetAsync(CacheKey key, object data)
        {
            return data != null
                ? GetOrSetAsync(key, () => Task.FromResult(data), true)
                : Task.CompletedTask;
        }

        /// <summary>
        /// Remove items by cache key prefix. The default implementation is rather inefficient, so it is recommended for
        /// subclasses to override this method.
        /// </summary>
        /// <param name="prefix">Cache key prefix</param>
        /// <param name="prefixParameters">Parameters to create cache key prefix</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual async Task RemoveByPrefixAsync(string prefix, params object[] prefixParameters)
        {
            var prefix_ = PrepareKeyPrefix(prefix, prefixParameters);
            var removedKeys = RemoveByPrefixInstanceData(prefix_);
            await Task.WhenAll(removedKeys.Select(key => RemoveAsync(key, false)));
        }

        /// <summary>
        /// Clear all cache data. The default implementation is rather inefficient, so it is recommended for
        /// subclasses to override this method.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        public virtual async Task ClearAsync()
        {
            await Task.WhenAll(_locksByKey.Keys.Select(key => RemoveAsync(key, false)));
            ClearInstanceData();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
