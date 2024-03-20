using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class DmBackedIngestionManager : IIngestionManager
    {
        #region IAsyncDisposable
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ValueTask.CompletedTask;
        }
        #endregion

        #region IIngestionManager
        Task IIngestionManager.QueueIngestionAsync(
            IEnumerable<Uri> blobUris,
            DateTime? creationTime)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
