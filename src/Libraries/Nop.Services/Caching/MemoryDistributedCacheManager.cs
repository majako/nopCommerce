using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Nop.Core.Caching;
using Nop.Core.Configuration;

namespace Nop.Services.Caching
{
    public class MemoryDistributedCacheManager : DistributedCacheManager
    {
        #region Ctor

        public MemoryDistributedCacheManager(AppSettings appSettings, IDistributedCache distributedCache, CacheLockManager cacheLockManager)
        : base(appSettings, distributedCache, cacheLockManager)
        {
        }

        #endregion

        public override async Task ClearAsync()
        {
            await Task.WhenAll(_cacheLockManager.Keys.Select(key => RemoveAsync(key, false)));
            ClearInstanceData();
        }

        public override async Task RemoveByPrefixAsync(string prefix, params object[] prefixParameters)
        {
            var prefix_ = PrepareKeyPrefix(prefix, prefixParameters);
            var removedKeys = RemoveByPrefixInstanceData(prefix_);
            await Task.WhenAll(removedKeys.Select(key => RemoveAsync(key, false)));
        }
    }
}
