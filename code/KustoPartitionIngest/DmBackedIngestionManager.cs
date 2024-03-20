using Azure.Core;
using Azure.Storage.Blobs.Models;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class DmBackedIngestionManager : IIngestionManager
    {
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly string _databaseName;
        private readonly string _tableName;

        public DmBackedIngestionManager(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            _databaseName = databaseName;
            _tableName = tableName;
        }

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
