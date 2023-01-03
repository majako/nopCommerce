using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace Nop.Core.Caching
{
    /// <summary>
    /// A distributed cache manager that locks the acquisition task
    /// </summary>
    public partial class MemoryCacheLocker : ILocker
    {
        #region Fields

        private readonly IMemoryCache _memoryCache;

        #endregion

        #region Ctor

        public MemoryCacheLocker(IMemoryCache memoryCache)
        {
            _memoryCache = memoryCache;
        }

        #endregion

        #region Utilities

        private async Task<bool> RunAsync(string key, TimeSpan expirationTime, Func<CancellationToken, Task> action, CancellationTokenSource cancellationTokenSource = default)
        {
            var started = false;
            try
            {
                var task = Task.CompletedTask;
                _ = _memoryCache.GetOrCreate(key, entry => new Lazy<CancellationTokenSource>(() =>
                {
                    entry.AbsoluteExpirationRelativeToNow = expirationTime;
                    var cts = cancellationTokenSource ?? new();
                    started = true;
                    task = action(cts.Token);
                    return cts;
                })).Value;
                await task;
            }
            catch (OperationCanceledException) { }
            finally
            {
                if (started)
                    _memoryCache.Remove(key);
            }
            return started;
        }

        #endregion

        #region Methods

        public async Task<bool> PerformActionWithLockAsync(string resource, TimeSpan expirationTime, Func<Task> action)
        {
            return await RunAsync(resource, expirationTime, _ => action());
        }

        public bool PerformActionWithLock(string resource, TimeSpan expirationTime, Action action)
        {
            return PerformActionWithLockAsync(resource, expirationTime, () => Task.Run(action)).Result;
        }

        public async Task RunWithHeartbeatAsync(string key, TimeSpan expirationTime, TimeSpan heartbeatInterval, Func<CancellationToken, Task> action, CancellationTokenSource cancellationTokenSource = default)
        {
            _ = await RunAsync(key, expirationTime, action, cancellationTokenSource);
        }

        public Task CancelTaskAsync(string key, TimeSpan expirationTime)
        {
            if (_memoryCache.TryGetValue(key, out CancellationTokenSource tokenSource))
                tokenSource.Cancel();
            return Task.CompletedTask;
        }

        public Task<bool> IsTaskRunningAsync(string key)
        {
            return Task.FromResult(_memoryCache.TryGetValue(key, out _));
        }

        #endregion
    }
}
