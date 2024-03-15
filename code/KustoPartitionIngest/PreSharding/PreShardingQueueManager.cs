using Azure.Core;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data.Common;
using Parquet;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoPartitionIngest.PreSharding
{
    internal class PreShardingQueueManager : QueueManagerBase
    {
        #region Inner types
        private record RowCountBlob(Uri blobUri, long rowCount);
        #endregion

        private const long MAX_ROW_COUNT = 1048576;
        private const int PARALLEL_COUNTING = 1;

        private readonly ConcurrentQueue<RowCountBlob> _rowCountBlobs = new();

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
            var allProcessTasks = Task.WhenAll(processTasks);

            await ProcessRowCountBlobsAsync(allProcessTasks);
            await allProcessTasks;
        }

        private async Task ProcessRowCountBlobsAsync(Task allProcessTasks)
        {
            var aggregationBuckets = new Dictionary<DateTime, List<RowCountBlob>>();

            while (!allProcessTasks.IsCompleted || _rowCountBlobs.Any())
            {
                if (_rowCountBlobs.TryDequeue(out var blob))
                {
                    var creationTime = ExtractTimeFromUri(blob.blobUri);

                    if (!aggregationBuckets.ContainsKey(creationTime))
                    {
                        aggregationBuckets[creationTime] = new();
                    }

                    var bucket = aggregationBuckets[creationTime];

                    if (bucket.Sum(i => i.rowCount) + blob.rowCount > MAX_ROW_COUNT
                        && bucket.Any())
                    {
                        await IngestBlobsAsync(creationTime, bucket.Select(i => i.blobUri));
                        bucket.Clear();
                    }
                    bucket.Add(blob);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
            //  Flush all buckets
            foreach(var pair in aggregationBuckets)
            {
                var creationTime = pair.Key;
                var bucket = pair.Value;
             
                await IngestBlobsAsync(creationTime, bucket.Select(i => i.blobUri));
            }
        }

        private async Task IngestBlobsAsync(
            DateTime creationTime,
            IEnumerable<Uri> batchBlobUris)
        {
            var batchId = Guid.NewGuid().ToString();

            await Task.WhenAll(batchBlobUris
                .Select(b => IngestBlobAsync(creationTime, b, batchId)));
        }

        private async Task IngestBlobAsync(
            DateTime creationTime,
            Uri blobUri,
            string batchId)
        {
            var timestamp = ExtractTimeFromUri(blobUri);
            var properties = CreateIngestionProperties();

            properties.AdditionalProperties.Add(
                "creationTime",
                $"{timestamp.Year}-{timestamp.Month}-{timestamp.Day}");
            properties.DropByTags = new[] { batchId };
            properties.Format = DataSourceFormat.parquet;

            await IngestFromStorageAsync(blobUri, properties);
        }

        private DateTime ExtractTimeFromUri(Uri blobUri)
        {
            var parts = blobUri.LocalPath.Split('/');
            var partitions = parts.TakeLast(5).Take(4);
            var partitionValues = partitions
                .Select(p => p.Split('=').Last());

            if (partitions.Count() == 4)
            {
                var year = GetInteger(partitionValues.First());
                var month = GetInteger(partitionValues.Skip(1).First());
                var day = GetInteger(partitionValues.Skip(2).First());
                var hour = GetInteger(partitionValues.Skip(3).First());
                var timestamp = GetTimestamp(year, month, day, hour);

                if (timestamp != null)
                {
                    return timestamp.Value;
                }
            }

            throw new InvalidDataException(
                $"Can't extract creation-time from URI '{blobUri}'");

            int? GetInteger(string text)
            {
                if (int.TryParse(text, out var value))
                {
                    return value;
                }
                else
                {
                    return null;
                }
            }

            DateTime? GetTimestamp(int? year, int? month, int? day, int? hour)
            {
                if (year != null && month != null && day != null && hour != null)
                {
                    return new DateTime(year.Value, month.Value, day.Value, hour.Value, 0, 0);
                }
                else
                {
                    return null;
                }
            }
        }

        private async Task ProcessUriAsync()
        {
            Uri? blobUri;

            while ((blobUri = await DequeueBlobUriAsync()) != null)
            {
                var rowCount = await FetchParquetRowCountAsync(blobUri);

                _rowCountBlobs.Enqueue(new RowCountBlob(blobUri, rowCount));
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