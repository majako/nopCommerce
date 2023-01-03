using System;
using System.Threading.Tasks;
using FluentAssertions;
using Nop.Core.Caching;
using NUnit.Framework;

namespace Nop.Tests.Nop.Core.Tests.Caching
{
    [TestFixture]
    public class MemoryCacheLockerTests : BaseNopTest
    {
        private MemoryCacheLocker _locker;

        [OneTimeSetUp]
        public void Setup()
        {
            _locker = GetService<ILocker>() as MemoryCacheLocker;
        }

        [Test]
        public async Task CanPerformLock()
        {
            var key = new CacheKey("Nop.Task");
            var expiration = TimeSpan.FromMinutes(2);

            var actionCount = 0;
            var action = new Func<Task>(async () =>
            {
                var result = await _locker.PerformActionWithLockAsync(key.Key, expiration,
                    () =>
                    {
                        Assert.Fail("Action in progress");
                        return Task.CompletedTask;
                    });
                    
                    result.Should().BeFalse();

                if (++actionCount % 2 == 0)
                    throw new ApplicationException("Alternating actions fail");
            });

            var result = await _locker.PerformActionWithLockAsync(key.Key, expiration, action);
            result.Should().BeTrue();
            actionCount.Should().Be(1);

            Assert.ThrowsAsync<AggregateException>(() => _locker.PerformActionWithLockAsync(key.Key, expiration, action));
            actionCount.Should().Be(2);

            result = await _locker.PerformActionWithLockAsync(key.Key, expiration, action);
            result.Should().BeTrue();
            actionCount.Should().Be(3);
        }
    }
}
