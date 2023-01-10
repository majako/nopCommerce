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

        public async Task RemoveLockAsync(string key)
        {
            if (_locksByKey.TryGetValue(key, out var lazy))
            {
                try
                {
                    await lazy.Value.WaitAsync();   // let current tasks finish, then lock
                    _locksByKey.TryRemove(key);
                    lazy.Value.Cancel();
                    // don't release, rely on cancellation token and let the GC take care of the semaphore
                }
                catch   // someone else cancelled first
                {
                }
            }
        }

        public void Clear()
        {
            _locksByKey.Clear();
        }

        public IEnumerable<string> RemoveByPrefix(string prefix)
        {
            return _locksByKey.Prune(prefix, out var subtree)
                ? subtree.Keys
                : Enumerable.Empty<string>();
        }
    }
}
