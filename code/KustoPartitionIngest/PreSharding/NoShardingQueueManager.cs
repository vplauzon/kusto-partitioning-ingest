using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoPartitionIngest.PreSharding
{
    internal class NoShardingQueueManager : SparkCreationTimeQueueManagerBase
    {
        private const int PARALLEL_QUEUING = 32;

        public NoShardingQueueManager(
            string name,
            IEnumerable<BlobEntry> blobList,
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
            : base(name, blobList, credentials, ingestionUri, databaseName, tableName)
        {
        }

        protected override async Task RunInternalAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_QUEUING)
                .Select(i => Task.Run(() => ProcessUriAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        private async Task ProcessUriAsync()
        {
            BlobEntry? blobEntry;

            while ((blobEntry = DequeueBlobEntry()) != null)
            {
                var timestamp = ExtractTimeFromUri(blobEntry.uri);
                var properties = CreateIngestionProperties();

                properties.AdditionalProperties.Add(
                    "creationTime",
                    $"{timestamp.Year:D2}-{timestamp.Month:D2}-{timestamp.Day:D2} "
                    + $"{timestamp.Hour:D2}:00:00.0000");
                properties.Format = DataSourceFormat.parquet;

                await IngestFromStorageAsync(blobEntry.uri, properties);
            }
        }
    }
}