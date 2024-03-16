using Azure.Core;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    public abstract class QueueManagerBase : IQueueManager
    {
        private bool _isCompleted = false;
        private readonly ConcurrentQueue<Uri> _blobUris = new();
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly string _tableName;

        public event EventHandler? BlobUriQueued;

        protected QueueManagerBase(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            DatabaseName = databaseName;
            _tableName = tableName;
        }

        protected string DatabaseName { get; }

        public async Task RunAsync()
        {
            await RunInternalAsync();
        }

        public void QueueUri(Uri blobUri)
        {
            _blobUris.Enqueue(blobUri);
        }

        public void Complete()
        {
            _isCompleted = true;
        }

        protected abstract Task RunInternalAsync();

        protected async Task<IEnumerable<Uri>> DequeueBlobUrisAsync(int maxCount)
        {
            var uris = new List<Uri>();

            while (uris.Count < maxCount)
            {
                if (_blobUris.TryDequeue(out var blobUri))
                {
                    uris.Add(blobUri);
                }
                else if (!uris.Any() && !_isCompleted)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }

            return uris;
        }

        protected async Task<Uri?> DequeueBlobUriAsync()
        {
            var uris = await DequeueBlobUrisAsync(1);

            return uris.FirstOrDefault();
        }

        protected KustoIngestionProperties CreateIngestionProperties()
        {
            return new KustoIngestionProperties(DatabaseName, _tableName);
        }

        protected async Task IngestFromStorageAsync(
            Uri blobUri,
            KustoIngestionProperties properties)
        {
            try
            {
            await _ingestClient.IngestFromStorageAsync(blobUri.ToString(), properties);
            RaiseBlobUriQueued();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error at ingestion:  {ex.Message}");
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