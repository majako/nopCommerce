using System.Threading;
using System.Threading.Tasks;

namespace Nop.Core.Caching
{
    public struct CacheLock
    {
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private readonly CancellationTokenSource _tokenSource = new();

        public CacheLock() { }

        public async Task WaitAsync()
        {
            await _semaphore.WaitAsync(_tokenSource.Token);
        }

        public void Release()
        {
            _semaphore.Release();
        }

        public void Cancel()
        {
            _tokenSource.Cancel();
        }
    }
}
