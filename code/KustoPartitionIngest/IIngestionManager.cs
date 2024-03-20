using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal interface IIngestionManager : IAsyncDisposable
    {
        Task QueueIngestionAsync(
            IEnumerable<Uri> blobUris,
            DateTime? creationTime);
    }
}