using Azure.Identity;

namespace KustoPartitionIngest
{
    internal class BulkOrchestrator
    {
        private readonly IQueueManager _queueManager1;
        private readonly IQueueManager? _queueManager2;
        private readonly BlobListManager _blobListManager;

        public BulkOrchestrator(
            IQueueManager queueManager1,
            IQueueManager? queueManager2,
            string storageUrl)
        {
            _queueManager1 = queueManager1;
            _queueManager2 = queueManager2;
            _blobListManager = new BlobListManager(storageUrl);
        }

        public async Task RunAsync()
        {
            _blobListManager.UriDiscovered += (sender, blobUri) =>
            {
                _queueManager1.QueueUri(blobUri);
                _queueManager2?.QueueUri(blobUri);
            };

            var reportManager =
                new ReportManager(_blobListManager, _queueManager1, _queueManager2);
            var listTask = _blobListManager.ListBlobsAsync();
            var queue1Task = _queueManager1.RunAsync();
            var queue2Task = _queueManager2?.RunAsync();
            var reportTask = reportManager.RunAsync();

            await listTask;
            _queueManager1.Complete();
            _queueManager2?.Complete();
            await queue1Task;
            if (queue2Task != null)
            {
                await queue2Task;
            }
            reportManager.Complete();
            await reportTask;
        }
    }
}