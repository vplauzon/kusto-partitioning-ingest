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
            DefaultAzureCredential credentials,
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
            while (_isCompleted)
            {
                if (_blobUris.TryDequeue(out var blobUri))
                {
                    RaiseBlobUriQueued();
                }
                else
                {   //  Wait for more blobs
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
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