using Azure.Core;
using System.Collections.Immutable;

namespace KustoPartitionIngest.Flat
{
    internal class FlatQueueManager : QueueManagerBase
    {
        private const int PARALLEL_QUEUING = 32;
        private readonly DmBackedIngestionManager _ingestionManager;

        public FlatQueueManager(
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
                .Select(i => Task.Run(() => ProcessUrisAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        protected override IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported)
        {
            return reported
                .Add("Queued", _ingestionManager.QueueCount.ToString());
        }

        private async Task ProcessUrisAsync()
        {
            BlobEntry? blobEntry;

            while ((blobEntry = DequeueBlobEntry()) != null)
            {
                await _ingestionManager.QueueIngestionAsync(
                    new[] { blobEntry.uri },
                    DateTime.Now);
            }
        }
    }
}