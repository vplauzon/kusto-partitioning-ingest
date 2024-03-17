using Azure.Core;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace KustoPartitionIngest
{
    public abstract class QueueManagerBase : IQueueManager
    {
        private readonly string _name;
        private readonly ConcurrentQueue<BlobEntry> _blobs;
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly string _tableName;
        private volatile int _queuedCount = 0;

        protected QueueManagerBase(
            string name,
            IEnumerable<BlobEntry> blobList,
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _name = name;
            _blobs = new(blobList);
            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            DatabaseName = databaseName;
            _tableName = tableName;
        }

        protected string DatabaseName { get; }

        #region IReportable
        string IReportable.Name => _name;

        IImmutableDictionary<string, string> IReportable.GetReport()
        {
            return AlterReported(ImmutableDictionary<string, string>
                .Empty
                .Add("Queued", _queuedCount.ToString()));
        }
        #endregion

        #region IQueueManager
        async Task IQueueManager.RunAsync()
        {
            await RunInternalAsync();
        }
        #endregion

        protected virtual IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported)
        {
            return reported;
        }

        protected abstract Task RunInternalAsync();

        protected BlobEntry? DequeueBlobEntry()
        {
            if (_blobs.TryDequeue(out var blob))
            {
                return blob;
            }
            else
            {
                return null;
            }
        }

        protected KustoIngestionProperties CreateIngestionProperties()
        {
            return new KustoIngestionProperties(DatabaseName, _tableName);
        }

        protected async Task IngestFromStorageAsync(
            Uri blobUri,
            KustoIngestionProperties properties)
        {
            await _ingestClient.IngestFromStorageAsync(blobUri.ToString(), properties);
            Interlocked.Increment(ref _queuedCount);
        }
    }
}