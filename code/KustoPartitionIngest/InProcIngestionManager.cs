using Azure.Core;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using KustoPartitionIngest.InProcManagedIngestion;
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
        private readonly IngestionCommandManager _commandManager;
        private readonly string _tableName;
        private readonly DataSourceFormat _format;

        public InProcIngestionManager(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName,
            DataSourceFormat format)
        {
            var queryUri = GetQueryUriFromIngestionUri(ingestionUri);
            var connectionBuilder = new KustoConnectionStringBuilder(queryUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);
            var commandClient = KustoClientFactory.CreateCslAdminProvider(connectionBuilder);

            _commandManager = new IngestionCommandManager(
                commandClient,
                databaseName);
            _tableName = tableName;
            _format = format;
        }

        #region IAsyncDisposable
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await using (_commandManager)
            {
            }
        }
        #endregion

        public static Uri GetQueryUriFromIngestionUri(Uri ingestionUri)
        {
            var uriBuilder = new UriBuilder(ingestionUri);

            uriBuilder.Host = uriBuilder.Host.Substring("ingest-".Length);

            return uriBuilder.Uri;
        }

        public async Task QueueIngestionAsync(
            ICompleter completer,
            IEnumerable<Uri> blobUris,
            DateTime? creationTime,
            params (string key, string value)[] properties)
        {
            var blobUriList = string.Join(
                ", ",
                blobUris
                .Select(u => $"'{u}'"));
            var commandText = $@"
.ingest async into table {_tableName}
({blobUriList})
with (format='{_format}')";

            await _commandManager.QueueIngestionAsync(completer, commandText);
        }
    }
}