using System;
using System.Threading.Tasks;

namespace Nop.Core.Caching
{
    public static class CachingExtensions
    {
        /// <summary>
        /// Get a cached item. If it's not in the cache yet, then load and cache it.
        /// NOTE: this method is only kept for backwards compatibility: the async overload is preferred!
        /// </summary>
        /// <typeparam name="T">Type of cached item</typeparam>
        /// <param name="key">Cache key</param>
        /// <param name="acquire">Function to load item if it's not in the cache yet</param>
        /// <returns>The cached value associated with the specified key</returns>
        public static T Get<T>(this IStaticCacheManager cacheManager,CacheKey key, Func<T> acquire)
        {
            return cacheManager.GetAsync(key, acquire).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Perform some action with exclusive lock.
        /// NOTE: this method is only kept for backwards compatibility: the async overload is preferred!
        /// </summary>
        /// <param name="resource">The key we are locking on</param>
        /// <param name="expirationTime">The time after which the lock will automatically be expired</param>
        /// <param name="action">Action to be performed with locking</param>
        /// <returns>True if lock was acquired and action was performed; otherwise false</returns>
        public static bool PerformActionWithLock(this ILocker locker, string resource, TimeSpan expirationTime, Action action)
        {
            return locker.PerformActionWithLockAsync(resource, expirationTime, () => Task.Run(action)).GetAwaiter().GetResult();
        }
    }
}
