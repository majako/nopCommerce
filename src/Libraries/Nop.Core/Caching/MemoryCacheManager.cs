using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Primitives;
using Nop.Core.Configuration;

namespace Nop.Core.Caching
{
    /// <summary>
    /// A memory cache manager that locks the acquisition task
    /// </summary>
    public partial class MemoryCacheManager : CacheKeyService, IStaticCacheManager
    {
        #region Fields

        // Flag: Has Dispose already been called?
        private bool _disposed;

        private readonly IMemoryCache _memoryCache;
        private static readonly ConcurrentDictionary<string, CancellationTokenSource> _prefixes = new();
        private static CancellationTokenSource _clearToken = new();

        #endregion

        #region Ctor

        public MemoryCacheManager(AppSettings appSettings, IMemoryCache memoryCache) : base(appSettings)
        {
            _memoryCache = memoryCache;
        }

        #endregion

        #region Utilities

        /// <summary>
        /// Prepare cache entry options for the passed key
        /// </summary>
        /// <param name="key">Cache key</param>
        /// <returns>Cache entry options</returns>
        private static MemoryCacheEntryOptions PrepareEntryOptions(CacheKey key)
        {
            //set expiration time for the passed cache key
            var options = new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(key.CacheTime)
            };

            //add tokens to clear cache entries
            options.AddExpirationToken(new CancellationChangeToken(_clearToken.Token));
            foreach (var keyPrefix in key.Prefixes.ToList())
            {
                var tokenSource = _prefixes.GetOrAdd(keyPrefix, new CancellationTokenSource());
                options.AddExpirationToken(new CancellationChangeToken(tokenSource.Token));
            }

            return options;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Remove the value with the specified key from the cache
        /// </summary>
        /// <param name="cacheKey">Cache key</param>
        /// <param name="cacheKeyParameters">Parameters to create cache key</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public Task RemoveAsync(CacheKey cacheKey, params object[] cacheKeyParameters)
        {
            _memoryCache.Remove(PrepareKey(cacheKey, cacheKeyParameters).Key);
            return Task.CompletedTask;
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
            if ((key?.CacheTime ?? 0) <= 0)
                return await acquire();

            var data = new Lazy<Task<T>>(acquire, true);
            return await _memoryCache.GetOrCreate(key.Key, entry =>
            {
                entry.SetOptions(PrepareEntryOptions(key));
                return data;
            }).Value;
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
        public async Task<T> GetAsync<T>(CacheKey key, Func<T> acquire)
        {
            return await GetAsync(key, () => Task.FromResult(acquire()));
        }

        /// <summary>
        /// Add the specified key and object to the cache
        /// </summary>
        /// <param name="key">Key of cached item</param>
        /// <param name="data">Value for caching</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public Task SetAsync<T>(CacheKey key, T data)
        {
            if (data != null && (key?.CacheTime ?? 0) > 0)
            {
                _memoryCache.Set(
                    key.Key,
                    new Lazy<Task<T>>(() => Task.FromResult(data), true),
                    PrepareEntryOptions(key));
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Remove items by cache key prefix
        /// </summary>
        /// <param name="prefix">Cache key prefix</param>
        /// <param name="prefixParameters">Parameters to create cache key prefix</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        public Task RemoveByPrefixAsync(string prefix, params object[] prefixParameters)
        {
            var prefix_ = PrepareKeyPrefix(prefix, prefixParameters);

            _prefixes.TryRemove(prefix_, out var tokenSource);
            tokenSource?.Cancel();
            tokenSource?.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Clear all cache data
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        public Task ClearAsync()
        {
            _clearToken.Cancel();
            _clearToken.Dispose();

            _clearToken = new CancellationTokenSource();

            foreach (var prefix in _prefixes.Keys.ToList())
            {
                _prefixes.TryRemove(prefix, out var tokenSource);
                tokenSource?.Dispose();
            }

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _clearToken.Dispose();
                // don't dispose of the MemoryCache, as it is injected
            }

            _disposed = true;
        }

        #endregion
    }
}
