using System;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Nop.Services.Caching
{
    public class RedisSynchronizedMemoryCache : IMemoryCache, IDisposable
    {
        private static readonly string _processId;

        private RedisConnectionWrapper _connection;

        private bool _disposed;
        private readonly IMemoryCache _memoryCache;
        private readonly string _ignorePrefix;

        private IDatabase RedisDb => _connection.GetDatabase();
        private string Channel => $"__change@{RedisDb.Database}__{Instance}__:";


        static RedisSynchronizedMemoryCache()
        {
            var machineId = NetworkInterface.GetAllNetworkInterfaces()
                .Where(nic => nic.OperationalStatus == OperationalStatus.Up)
                .Select(nic => nic.GetPhysicalAddress().ToString()).FirstOrDefault();

            if (string.IsNullOrEmpty(machineId))
                machineId = Environment.MachineName;

            _processId = machineId + Environment.ProcessId.ToString();
        }


        public RedisSynchronizedMemoryCache(IMemoryCache memoryCache, RedisConnectionWrapper connectionWrapper, string ignorePrefix = null)
        {
            _memoryCache = memoryCache;
            _ignorePrefix = ignorePrefix;
            _connection = connectionWrapper;
            _ = SubscribeAsync();
        }

        private async Task SubscribeAsync()
        {
            (await _connection.GetSubscriber()).Subscribe(Channel + "*", (channel, value) =>
            {
                if (value != _processId)
                    _memoryCache.Remove(((string)channel).Replace(Channel, ""));
            });
        }

        private async Task PublishChangeEventAsync(object key)
        {
            var stringKey = key.ToString();
            if (string.IsNullOrEmpty(_ignorePrefix) || !stringKey.StartsWith(_ignorePrefix))
                await Subscriber.PublishAsync($"{Channel}{stringKey}", _processId, CommandFlags.FireAndForget);
        }

        private void OnEviction(object key, object value, EvictionReason reason, object state)
        {
            switch (reason)
            {
                case EvictionReason.Replaced:
                case EvictionReason.TokenExpired: // e.g. clear cache event
                    _ = PublishChangeEventAsync(key);
                    break;
                // don't publish here on removed, as it could be triggered by a redis event itself
                default:
                    break;
            }
        }

        public ICacheEntry CreateEntry(object key)
        {
            return _memoryCache.CreateEntry(key).RegisterPostEvictionCallback(OnEviction);
        }

        public void Remove(object key)
        {
            _memoryCache.Remove(key);
            // publish event manually instead of through eviction callback to avoid feedback loops
            _ = PublishChangeEventAsync(key);
        }

        public bool TryGetValue(object key, out object value)
        {
            return _memoryCache.TryGetValue(key, out value);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    Subscriber.Unsubscribe(Channel + "*");
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
