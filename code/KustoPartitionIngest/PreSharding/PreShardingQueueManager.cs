using Azure.Core;
using Azure.Storage.Blobs.Specialized;
using Parquet;
using System.Collections.Immutable;

namespace KustoPartitionIngest.PreSharding
{
    internal class PreShardingQueueManager : QueueManagerBase
    {
        private const int PARALLEL_COUNTING = 1;

        public PreShardingQueueManager(
            TokenCredential credentials,
            string ingestionUri,
            string databaseName,
            string tableName)
            : base(credentials, ingestionUri, databaseName, tableName)
        {
        }

        protected override async Task RunInternalAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_COUNTING)
                .Select(i => Task.Run(() => ProcessUriAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        private async Task ProcessUriAsync()
        {
            Uri? blobUri;

            while ((blobUri = await DequeueBlobUriAsync()) != null)
            {
                var rowCount = await FetchParquetRowCountAsync(blobUri);
            }
        }

        private static async Task<long> FetchParquetRowCountAsync(Uri blobUri)
        {
            var blobClient = new BlockBlobClient(blobUri);

            using (var stream = await blobClient.OpenReadAsync())
            using (var parquetReader = await ParquetReader.CreateAsync(stream))
            {
                if (parquetReader == null || parquetReader.Metadata == null)
                {
                    throw new InvalidOperationException("Impossible to read parquet");
                }
                
                return parquetReader.Metadata.NumRows;
            }
        }
    }
}