using Azure.Core;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class DmBackedIngestionManager
    {
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly string _databaseName;
        private readonly string _tableName;
        private readonly DataSourceFormat _format;
        private volatile int _queuedCount = 0;

        public DmBackedIngestionManager(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName,
            DataSourceFormat format)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            _databaseName = databaseName;
            _tableName = tableName;
            _format = format;
        }

        public int QueueCount => _queuedCount;

        public async Task QueueIngestionAsync(
            IEnumerable<Uri> blobUris,
            DateTime? creationTime,
            params (string key, string value)[] properties)
        {
            var kustoProperties = new KustoIngestionProperties(
                _databaseName,
                _tableName);

            kustoProperties.Format = _format;
            if (creationTime != null)
            {
                var time = creationTime.Value;

                kustoProperties.AdditionalProperties.Add(
                    "creationTime",
                    $"{time.Year:D2}-{time.Month:D2}-{time.Day:D2} "
                    + $"{time.Hour:D2}:{time.Minute:D2}:{time.Second:D2}.{time.Millisecond:D4}");
            }
            foreach (var p in properties)
            {
                kustoProperties.AdditionalProperties.Add(p.key, p.value);
            }

            var ingestTasks = blobUris
                .Select(b => _ingestClient.IngestFromStorageAsync(b.ToString(), kustoProperties))
                .ToImmutableArray();

            await Task.WhenAll(ingestTasks);
            Interlocked.Add(ref _queuedCount, ingestTasks.Count());
        }
    }
}
