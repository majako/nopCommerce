﻿using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Nop.Core.Caching;
using NUnit.Framework;

namespace Nop.Tests.Nop.Core.Tests.Caching
{
    [TestFixture]
    public class MemoryCacheManagerTests : BaseNopTest
    {
        private MemoryCacheManager _staticCacheManager;

        [OneTimeSetUp]
        public void Setup()
        {
            _staticCacheManager = GetService<IStaticCacheManager>() as MemoryCacheManager;
        }

        [TearDown]
        public async Task TaskTearDown()
        {
            await _staticCacheManager.ClearAsync();
        }

        [Test]
        public async Task CanSetAndGetObjectFromCache()
        {
            await _staticCacheManager.SetAsync(new CacheKey("some_key_1"), 3);
            var rez = await _staticCacheManager.GetAsync(new CacheKey("some_key_1"), () => 0);
            rez.Should().Be(3);
        }

        [Test]
        public async Task CanValidateWhetherObjectIsCached()
        {
            await _staticCacheManager.SetAsync(new CacheKey("some_key_1"), 3);
            await _staticCacheManager.SetAsync(new CacheKey("some_key_2"), 4);

            var rez = await _staticCacheManager.GetAsync(new CacheKey("some_key_1"), () => 2);
            rez.Should().Be(3);
            rez = await _staticCacheManager.GetAsync(new CacheKey("some_key_2"), () => 2);
            rez.Should().Be(4);
        }

        [Test]
        public async Task CanClearCache()
        {
            await _staticCacheManager.SetAsync(new CacheKey("some_key_1"), 3);

            await _staticCacheManager.ClearAsync();

            var rez = await _staticCacheManager.GetAsync(new CacheKey("some_key_1"), () => Task.FromResult((object)null));
            rez.Should().BeNull();
        }

        [Test]
        public async Task ExecutesSetInOrder()
        {
            await Task.WhenAll(Enumerable.Range(1, 5).Select(i => _staticCacheManager.SetAsync(new CacheKey("some_key_1"), i)));
            var value = await _staticCacheManager.GetAsync(new CacheKey("some_key_1"), () => Task.FromResult(0));
            value.Should().Be(5);
        }

        [Test]
        public async Task GetsLazily()
        {
            var xs = new int[5];
            await Task.WhenAll(xs.Select((_, i) => _staticCacheManager.GetAsync(
                new CacheKey("some_key_1"),
                async () =>
                {
                    xs[i] = 1;
                    await Task.Delay(10);
                    return i;
                })));
            var value = await _staticCacheManager.GetAsync(new CacheKey("some_key_1"), () => Task.FromResult(-1));
            value.Should().Be(0);
            xs.Sum().Should().Be(1);
        }

        [Test]
        public void ThrowsException()
        {
            Assert.ThrowsAsync<ApplicationException>(() => _staticCacheManager.GetAsync(
                new CacheKey("some_key_1"),
                Task<object> () => throw new ApplicationException()));
        }
    }
}
