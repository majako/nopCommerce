using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.AspNetCore.Http;
using Nop.Core.ComponentModel;

namespace Nop.Core.Caching
{
    /// <summary>
    /// Represents a manager for caching during an HTTP request (short term caching)
    /// </summary>
    public class PerRequestCache
    {
        #region Fields

        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly ReaderWriterLockSlim _lockSlim;

        #endregion

        #region Ctor

        public PerRequestCache(IHttpContextAccessor httpContextAccessor)
        {
            _httpContextAccessor = httpContextAccessor;

            _lockSlim = new ReaderWriterLockSlim();
        }

        #endregion

        #region Utilities

        /// <summary>
        /// Get a key/value collection that can be used to share data within the scope of this request
        /// </summary>
        protected virtual IDictionary<object, object> GetItems()
        {
            return _httpContextAccessor.HttpContext?.Items;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Get a cached item. If it's not in the cache yet, then load and cache it
        /// </summary>
        /// <typeparam name="T">Type of cached item</typeparam>
        /// <param name="key">Cache key</param>
        /// <param name="acquire">Function to load item if it's not in the cache yet</param>
        /// <returns>The cached value associated with the specified key</returns>
        public virtual T Get<T>(string key, Func<T> acquire)
        {
            IDictionary<object, object> items;

            using (new ReaderWriteLockDisposable(_lockSlim, ReaderWriteLockType.Read))
            {
                items = GetItems();
                if (items == null)
                    return acquire();

                //item already is in cache, so return it
                if (items[key] != null)
                    return (T)items[key];
            }

            //or create it using passed function
            var result = acquire();

            //and set in cache (if cache time is defined)
            using (new ReaderWriteLockDisposable(_lockSlim))
                items[key] = result;

            return result;
        }

        /// <summary>
        /// Add the specified key and object to the cache
        /// </summary>
        /// <param name="key">Key of cached item</param>
        /// <param name="data">Value for caching</param>
        public virtual void Set(string key, object data)
        {
            if (data == null)
                return;

            using (new ReaderWriteLockDisposable(_lockSlim))
            {
                var items = GetItems();
                if (items == null)
                    return;

                items[key] = data;
            }
        }

        /// <summary>
        /// Get a value indicating whether the value associated with the specified key is cached
        /// </summary>
        /// <param name="key">Key of cached item</param>
        /// <returns>True if item already is in cache; otherwise false</returns>
        public virtual bool IsSet(string key)
        {
            using (new ReaderWriteLockDisposable(_lockSlim, ReaderWriteLockType.Read))
            {
                var items = GetItems();
                return items?[key] != null;
            }
        }

        /// <summary>
        /// Remove the value with the specified key from the cache
        /// </summary>
        /// <param name="key">Key of cached item</param>
        public virtual void Remove(string key)
        {
            using (new ReaderWriteLockDisposable(_lockSlim))
            {
                var items = GetItems();
                items?.Remove(key);
            }
        }

        /// <summary>
        /// Remove items by key prefix
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        public virtual void RemoveByPrefix(string prefix)
        {
            using (new ReaderWriteLockDisposable(_lockSlim, ReaderWriteLockType.UpgradeableRead))
            {
                var items = GetItems();
                if (items == null)
                    return;

                //get cache keys that matches pattern
                var regex = new Regex(prefix,
                    RegexOptions.Singleline | RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var matchesKeys = items.Keys.Select(p => p.ToString())
                    .Where(key => regex.IsMatch(key ?? string.Empty)).ToList();

                if (!matchesKeys.Any())
                    return;

                using (new ReaderWriteLockDisposable(_lockSlim))
                    //remove matching values
                    foreach (var key in matchesKeys)
                        items.Remove(key);
            }
        }

        #endregion
    }
}
