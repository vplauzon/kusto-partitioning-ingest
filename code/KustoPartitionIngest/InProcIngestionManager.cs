using Azure.Core;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class InProcIngestionManager : IAsyncDisposable
    {
        private readonly string _tableName;
        private readonly DataSourceFormat _format;
        private volatile int _queuedCount = 0;
        private volatile int _ingestedCount = 0;

        public InProcIngestionManager(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName,
            DataSourceFormat format)
        {
            var uriBuilder = new UriBuilder(ingestionUri);

            uriBuilder.Host = uriBuilder.Host.Substring("ingest-".Length);

            var connectionBuilder = new KustoConnectionStringBuilder(uriBuilder.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            QueryClient = KustoClientFactory.CreateCslQueryProvider(connectionBuilder);
            DatabaseName = databaseName;
            _tableName = tableName;
            _format = format;
        }

        public string DatabaseName { get; }

        public ICslQueryProvider QueryClient { get; }

        public int QueueCount => _queuedCount;

        public int IngestedCount => _ingestedCount;

        public void QueueIngestion(
            IEnumerable<Uri> blobUris,
            DateTime? creationTime,
            params (string key, string value)[] properties)
        {
            var uris = blobUris.ToImmutableArray();

            Interlocked.Add(ref _queuedCount, uris.Length);

            throw new NotImplementedException();
        }

        #region IAsyncDisposable
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}