using System.Threading;
using System.Threading.Tasks;

namespace Nop.Core.Caching
{
    public struct CacheLock
    {
        private readonly SemaphoreSlim _semaphore;
        private readonly CancellationTokenSource _tokenSource;

        public CacheLock()
        {
            _tokenSource = new();
            _semaphore = new(1, 1);
        }

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
