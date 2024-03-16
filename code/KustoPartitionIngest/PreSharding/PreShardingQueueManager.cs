using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
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
        private const int PARALLEL_COUNTING = 32;
        private const int MAX_BLOB_COUNT_COUNTING = 100;

        private readonly ConcurrentQueue<RowCountBlob> _rowCountBlobs = new();
        private readonly ICslQueryProvider _queryClient;

        public PreShardingQueueManager(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
            : base(credentials, ingestionUri, databaseName, tableName)
        {
            var uriBuilder = new UriBuilder(ingestionUri);

            uriBuilder.Host = uriBuilder.Host.Substring("ingest-".Length);

            var connectionBuilder = new KustoConnectionStringBuilder(uriBuilder.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _queryClient = KustoClientFactory.CreateCslQueryProvider(connectionBuilder);
        }

        protected override async Task RunInternalAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_COUNTING)
                .Select(i => Task.Run(() => EnrichBlobMetadataAsync()))
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
            foreach (var pair in aggregationBuckets)
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

        private async Task EnrichBlobMetadataAsync()
        {
            while (true)
            {
                var blobUris = await DequeueBlobUrisAsync(MAX_BLOB_COUNT_COUNTING);

                if (blobUris.Any())
                {
                    var rowCounts = await FetchParquetRowCountAsync(blobUris);
                    var enrichedBlobs = blobUris
                        .Zip(rowCounts, (blobUri, rowCount) => new RowCountBlob(
                            blobUri,
                            rowCount));

                    foreach (var b in enrichedBlobs)
                    {
                        _rowCountBlobs.Enqueue(b);
                    }
                }
                else
                {
                    return;
                }
            }
        }

        private async Task<IEnumerable<long>> FetchParquetRowCountAsync(
            IEnumerable<Uri> blobUris)
        {
            var counter = Enumerable.Range(0, blobUris.Count());
            var scalars = blobUris
                .Zip(counter, (uri, i) => new { uri, i })
                .Select(p => @$"let Count{p.i} = toscalar(externaldata(Timestamp:datetime)
    [
        '{p.uri}'
    ]
    with (format=""parquet"")
    | count);");
            var scalerPrint = counter
                .Select(i => $"Count{i}");
            var query = string.Join(Environment.NewLine, scalars)
                + "print "
                + string.Join(", ", scalerPrint);
            var reader = await _queryClient.ExecuteQueryAsync(
                DatabaseName,
                query,
                new ClientRequestProperties());
            var counts = reader.ToDataSet().Tables[0].Rows[0].ItemArray
                .Cast<long>();

            return counts;
        }
    }
}