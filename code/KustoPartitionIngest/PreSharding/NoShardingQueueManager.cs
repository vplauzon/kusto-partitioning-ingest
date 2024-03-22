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
        private readonly DmBackedIngestionManager _ingestionManager;

        public NoShardingQueueManager(
            string name,
            IEnumerable<BlobEntry> blobList,
            DmBackedIngestionManager ingestionManager)
            : base(name, blobList)
        {
            _ingestionManager = ingestionManager;
        }

        protected override async Task RunInternalAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_QUEUING)
                .Select(i => Task.Run(() => ProcessUriAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        protected override IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported)
        {
            return reported
                .Add("Queued", _ingestionManager.QueueCount.ToString());
        }

        private async Task ProcessUriAsync()
        {
            BlobEntry? blobEntry;

            while ((blobEntry = DequeueBlobEntry()) != null)
            {
                var timestamp = ExtractTimeFromUri(blobEntry.uri);

                await _ingestionManager.QueueIngestionAsync(new[] {blobEntry.uri}, timestamp);
            }
        }
    }
}