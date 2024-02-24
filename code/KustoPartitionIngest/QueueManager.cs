using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using Kusto.Ingest;
using System.Collections.Concurrent;

namespace KustoPartitionIngest
{
    internal class QueueManager
    {
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly bool _hasPartitioningHint;
        private readonly string _databaseName;
        private readonly string _tableName;
        private readonly ConcurrentQueue<Uri> _blobUris = new();
        private bool _isCompleted = false;

        public event EventHandler? BlobUriQueued;

        public QueueManager(
            TokenCredential credentials,
            bool hasPartitioningHint,
            string ingestionUri,
            string databaseName,
            string tableName)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri)
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            _hasPartitioningHint = hasPartitioningHint;
            _databaseName = databaseName;
            _tableName = tableName;
        }

        public void QueueUri(Uri blobUri)
        {
            _blobUris.Enqueue(blobUri);
        }

        public void Complete()
        {
            _isCompleted = true;
        }

        public async Task RunAsync()
        {
            while (!_isCompleted || _blobUris.Any())
            {
                if (_blobUris.TryDequeue(out var blobUri))
                {
                    (var timestamp, var partitionKey) = AnalyzeUri(blobUri);
                    var properties = new KustoIngestionProperties(_databaseName, _tableName);

                    await _ingestClient.IngestFromStorageAsync($"{blobUri}", properties);
                    RaiseBlobUriQueued();
                }
                else
                {   //  Wait for more blobs
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }

        private (DateTime? timestamp, string? partitionKey) AnalyzeUri(Uri blobUri)
        {
            return (null, null);
        }

        private void RaiseBlobUriQueued()
        {
            if (BlobUriQueued != null)
            {
                BlobUriQueued(this, EventArgs.Empty);
            }
        }
    }
}