
using System.Collections.Immutable;

namespace KustoPartitionIngest
{
    internal class ReportManager
    {
        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(5);

        private readonly BlobListManager _blobListManager;
        private readonly IImmutableList<QueueManager> _queueManagers;
        private readonly TaskCompletionSource _completionSource = new();

        public ReportManager(
            BlobListManager blobListManager,
            params QueueManager?[] queueManagers)
        {
            _blobListManager = blobListManager;
            _queueManagers = queueManagers
                .Where(q => q != null)
                .ToImmutableArray();
        }

        public void Complete()
        {
            _completionSource.SetResult();
        }

        public async Task RunAsync()
        {
            var listCount = 0;
            var queueCounts = new int[_queueManagers.Count];

            _blobListManager.UriDiscovered += (sender, blobUri) =>
            {
                Interlocked.Increment(ref listCount);
            };
            for (int i = 0; i != queueCounts.Length; ++i)
            {
                var index = i;

                _queueManagers[i].BlobUriQueued += (sender, e) =>
                {
                    Interlocked.Increment(ref queueCounts[index]);
                };
            }
            while (!_completionSource.Task.IsCompleted)
            {
                await Task.WhenAny(_completionSource.Task, Task.Delay(PERIOD));
                Console.WriteLine($"Queued:  {string.Join(", ", queueCounts)}");
                Console.WriteLine($"Discovered:  {listCount}");
                Console.WriteLine();
            }
        }
    }
}