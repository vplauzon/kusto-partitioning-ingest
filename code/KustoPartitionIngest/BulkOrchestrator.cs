﻿
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
            var credentials = new DefaultAzureCredential(true);

            _blobListManager = new BlobListManager(credentials, storageUrl);
            _queueManager1 = new QueueManager(
                credentials,
                true,
                ingestionUri1,
                tableName,
                databaseName);
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
                Console.WriteLine(blobUri);
            };
            await _blobListManager.ListBlobsAsync();
        }
    }
}