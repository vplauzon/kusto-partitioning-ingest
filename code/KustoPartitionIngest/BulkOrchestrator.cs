
using Azure.Identity;

namespace KustoPartitionIngest
{
    internal class BulkOrchestrator
    {
        private readonly BlobListManager _blobListManager;
        private readonly QueueManager _queueManager1;
        private readonly QueueManager? _queueManager2;

        public BulkOrchestrator(
            string storageUrl,
            string tableName,
            string databaseName,
            string ingestionUri1,
            string ingestionUri2)
        {
            var credentials = new AzureCliCredential();

            _blobListManager = new BlobListManager(storageUrl);
            _queueManager1 = new QueueManager(
                credentials,
                true,
                ingestionUri1,
                databaseName,
                tableName);
            _queueManager2 = string.IsNullOrWhiteSpace(ingestionUri2)
                ? null
                : new QueueManager(
                    credentials,
                    false,
                    ingestionUri2,
                    databaseName,
                    tableName);
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