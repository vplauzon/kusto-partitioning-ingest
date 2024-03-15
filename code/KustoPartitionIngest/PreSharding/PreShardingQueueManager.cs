using Azure.Core;
using Kusto.Data;
using Kusto.Ingest;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoPartitionIngest.PreSharding
{
    internal class PreShardingQueueManager : QueueManagerBase
    {
        private const int PARALLEL_COUNTING = 1;

        public PreShardingQueueManager(
            TokenCredential credentials,
            string ingestionUri,
            string databaseName,
            string tableName)
            : base(credentials, ingestionUri, databaseName, tableName)
        {
        }

        protected override async Task RunInternalAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_COUNTING)
                .Select(i => Task.Run(() => ProcessUriAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        private async Task ProcessUriAsync()
        {
            Uri? blobUri;

            while ((blobUri = await DequeueBlobUriAsync()) != null)
            {
            }
        }
    }
}