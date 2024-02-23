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
            throw new NotImplementedException();
        }

        public Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}