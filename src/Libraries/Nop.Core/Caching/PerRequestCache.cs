using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Nop.Core.Caching
{
    /// <summary>
    /// Represents a manager for caching during an HTTP request (short term caching)
    /// </summary>
    public class PerRequestCache
    {
        #region Fields

        private readonly ConcurrentDictionary<string, object> _items = new();

        public ICollection<string> Keys => _items.Keys;

        #endregion

        #region Utilities

        #endregion

        #region Methods

        public virtual void Clear()
        {
            _items.Clear();
        }

        public virtual IEnumerable<string> GetKeysByPrefix(string prefix)
        {
            var regex = new Regex(prefix,
                RegexOptions.Singleline | RegexOptions.Compiled | RegexOptions.IgnoreCase);

            return Keys.Where(key => regex.IsMatch(key));
        }

        /// <summary>
        /// Get a cached item. If it's not in the cache yet, then load and cache it
        /// </summary>
        /// <typeparam name="T">Type of cached item</typeparam>
        /// <param name="key">Cache key</param>
        /// <param name="value">The value to be added, if the key does not already exist</param>
        /// <returns>The cached value associated with the specified key</returns>
        public virtual T GetOrAdd<T>(string key, Func<T> valueFactory)
        {
            return (T)_items.GetOrAdd(key, valueFactory);
        }

        public virtual bool TryGetValue<T>(string key, out T value)
        {
            var exists = _items.TryGetValue(key, out var obj);
            value = (T)obj;
            return exists;
        }

        /// <summary>
        /// Add the specified key and object to the cache
        /// </summary>
        /// <param name="key">Key of cached item</param>
        /// <param name="data">Value for caching</param>
        public virtual void Set(string key, object data)
        {
            if (data != null)
                _items.AddOrUpdate(key, data, (k, oldValue) => data);
        }

        /// <summary>
        /// Get a value indicating whether the value associated with the specified key is cached
        /// </summary>
        /// <param name="key">Key of cached item</param>
        /// <returns>True if item already is in cache; otherwise false</returns>
        public virtual bool IsSet(string key)
        {
            return _items.TryGetValue(key, out var value) && value != null;
        }

        /// <summary>
        /// Remove the value with the specified key from the cache
        /// </summary>
        /// <param name="key">Key of cached item</param>
        public virtual void Remove(string key)
        {
            _items.Remove(key, out _);
        }

        /// <summary>
        /// Remove items by key prefix
        /// </summary>
        /// <param name="prefix">String key prefix</param>
        public virtual void RemoveByPrefix(string prefix)
        {
            foreach (var key in GetKeysByPrefix(prefix))
                _items.Remove(key, out _);
        }

        #endregion
    }
}
