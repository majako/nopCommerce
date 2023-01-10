using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nop.Core.Infrastructure;

namespace Nop.Core.Caching
{
    public class CacheLockManager
    {
        private readonly ConcurrentTrie<Lazy<CacheLock>> _locksByKey = new();

        public IEnumerable<string> Keys => _locksByKey.Keys;

        public async Task<CacheLock> AcquireLockAsync(string key)
        {
            while (true)
            {
                var cacheLock = _locksByKey.GetOrAdd(key, () => new Lazy<CacheLock>(true)).Value;
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

        public async Task RemoveLockAsync(string key)
        {
            if (_locksByKey.TryGetValue(key, out var lazy))
            {
                await CancelAsync(lazy.Value, () => _locksByKey.TryRemove(key));
            }
        }

        public void Clear()
        {
            _locksByKey.Clear();
        }

        public async Task<IEnumerable<string>> RemoveByPrefixAsync(string prefix)
        {
            if (!_locksByKey.Prune(prefix, out var subtree))
                return Enumerable.Empty<string>();
            await Task.WhenAll(subtree.Values.Select(v => CancelAsync(v.Value)));
            return subtree.Keys;
        }

        private static async Task CancelAsync(CacheLock cacheLock, Action action = null)
        {
            try
            {
                await cacheLock.WaitAsync();   // let current tasks finish, then lock
                action?.Invoke();
                cacheLock.Cancel();
            }
            catch   // someone else cancelled first
            {
            }
        }
    }
}
