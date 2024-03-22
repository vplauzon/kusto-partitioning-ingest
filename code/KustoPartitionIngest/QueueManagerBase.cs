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

        protected QueueManagerBase(
            string name,
            IEnumerable<BlobEntry> blobList)
        {
            _name = name;
            _blobs = new(blobList);
        }
        
        #region IReportable
        string IReportable.Name => _name;

        IImmutableDictionary<string, string> IReportable.GetReport()
        {
            return AlterReported(ImmutableDictionary<string, string>.Empty);
        }
        #endregion

        #region IQueueManager
        async Task IQueueManager.RunAsync()
        {
            await RunInternalAsync();
        }
        #endregion

        protected abstract IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported);

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
    }
}