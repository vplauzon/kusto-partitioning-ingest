
namespace KustoPartitionIngest
{
    internal class BulkOrchestrator
    {
        private readonly string _storageUrl;
        private readonly string _tableName;
        private readonly string _databaseName;
        private readonly string _ingestionUri1;
        private readonly string _ingestionUri2;

        public BulkOrchestrator(
            string storageUrl,
            string tableName,
            string databaseName,
            string ingestionUri1,
            string ingestionUri2)
        {
            _storageUrl = storageUrl;
            _tableName = tableName;
            _databaseName = databaseName;
            _ingestionUri1 = ingestionUri1;
            _ingestionUri2 = ingestionUri2;
        }

        public Task RunAsync()
        {
            return Task.CompletedTask;
        }
    }
}