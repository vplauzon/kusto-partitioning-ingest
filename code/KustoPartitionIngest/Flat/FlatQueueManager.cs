using Azure.Core;
using Microsoft.IdentityModel.Abstractions;
using System.Collections.Immutable;

namespace KustoPartitionIngest.Flat
{
    internal class FlatQueueManager : IQueueManager
    {
        private const int PARALLEL_QUEUING = 32;

        private readonly string _name;
        private readonly IImmutableList<BlobEntry> _blobList;
        private readonly DmBackedIngestionManager _ingestionManager;

        public FlatQueueManager(
            string name,
            IEnumerable<BlobEntry> blobList,
            DmBackedIngestionManager ingestionManager)
        {
            _name = name;
            _blobList = blobList.ToImmutableArray();
            _ingestionManager = ingestionManager;
        }

        #region IQueueManager
        string IReportable.Name => _name;

        async Task IQueueManager.RunAsync()
        {
            foreach (var chunk in _blobList.Chunk(PARALLEL_QUEUING))
            {
                var uris = chunk
                    .Select(e => e.uri);

                await _ingestionManager.QueueIngestionAsync(uris, null);
            }
        }

        IImmutableDictionary<string, string> IReportable.GetReport()
        {
            return ImmutableDictionary<string, string>
                .Empty
                .Add("Queued", _ingestionManager.QueueCount.ToString());
        }
        #endregion
    }
}