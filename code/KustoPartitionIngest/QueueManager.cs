using Azure.Identity;
using Kusto.Data;
using Kusto.Ingest;

namespace KustoPartitionIngest
{
    internal class QueueManager
    {
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly bool _hasPartitioningHint;
        private readonly string _databaseName;
        private readonly string _tableName;

        public QueueManager(
            DefaultAzureCredential credentials,
            bool hasPartitioningHint,
            string ingestionUri,
            string databaseName,
            string tableName)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri)
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            _hasPartitioningHint = hasPartitioningHint;
            _databaseName = databaseName;
            _tableName = tableName;
        }
    }
}