using Azure.Identity;
using System.Collections.Immutable;

namespace KustoPartitionIngest
{
    internal class BulkOrchestrator
    {
        private readonly Func<IEnumerable<BlobEntry>, IQueueManager> _queueManagerFactory1;
        private readonly Func<IEnumerable<BlobEntry>, IQueueManager?> _queueManagerFactory2;
        private readonly BlobListManager _blobListManager;

        public BulkOrchestrator(
            Func<IEnumerable<BlobEntry>, IQueueManager> queueManagerFactory1,
            Func<IEnumerable<BlobEntry>, IQueueManager?> queueManagerFactory2,
            string storageUrl)
        {
            _queueManagerFactory1 = queueManagerFactory1;
            _queueManagerFactory2 = queueManagerFactory2;
            _blobListManager = new BlobListManager(storageUrl);
        }

        public async Task RunAsync()
        {
            var blobList = await ListBlobsAsync();
            var queueManager1 = _queueManagerFactory1(blobList);
            var queueManager2 = _queueManagerFactory2(blobList);
            var reportManager = new ReportManager(queueManager1, queueManager2);
            var queue1Task = queueManager1.RunAsync();
            var queue2Task = queueManager2?.RunAsync();
            var allQueuesTask = queue2Task == null
                ? queue1Task
                : Task.WhenAll(queue1Task, queue2Task);
            var reportTask = reportManager.RunAsync(allQueuesTask);

            await allQueuesTask;
            await reportTask;
        }

        private async Task<IImmutableList<BlobEntry>> ListBlobsAsync()
        {
            var listTask = _blobListManager.ListBlobsAsync();
            var reportManager = new ReportManager(_blobListManager);
            var reportTask = reportManager.RunAsync(listTask);

            try
            {
                return await listTask;
            }
            finally
            {
                await reportTask;
            }
        }
    }
}