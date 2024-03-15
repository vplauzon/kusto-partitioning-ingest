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
        private readonly IKustoQueuedIngestClient IngestClient;
        private readonly string _databaseName;
        private readonly string _tableName;

        public event EventHandler? BlobUriQueued;

        protected QueueManagerBase(
            TokenCredential credentials,
            string ingestionUri,
            string databaseName,
            string tableName)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri)
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            IngestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            _databaseName = databaseName;
            _tableName = tableName;
        }

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

        protected async Task<Uri?> DequeueBlobUriAsync()
        {
            if (_isCompleted)
            {
                return null;
            }
            else if (_blobUris.TryDequeue(out var blobUri))
            {
                return blobUri;
            }
            else
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                return await DequeueBlobUriAsync();
            }
        }

        protected KustoIngestionProperties CreateIngestionProperties()
        {
            return new KustoIngestionProperties(_databaseName, _tableName);
        }

        protected async Task IngestFromStorageAsync(
            Uri blobUri,
            KustoIngestionProperties properties)
        {
            await IngestClient.IngestFromStorageAsync(blobUri.ToString(), properties);
            RaiseBlobUriQueued();
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