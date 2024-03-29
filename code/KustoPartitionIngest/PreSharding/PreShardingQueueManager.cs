﻿using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoPartitionIngest.PreSharding
{
    internal class PreShardingQueueManager : SparkCreationTimeQueueManagerBase
    {
        #region Inner types
        private record RowCountBlob(BlobEntry blobEntry, long rowCount);

        private record AggregationBucket(List<RowCountBlob> blobs, int lastWholeShardLength);
        #endregion

        private const long SHARD_ROW_COUNT = 1048576;
        private const long BATCH_SIZE = 1000 * 1000 * 1000;
        private const int PARALLEL_COUNTING = 15;
        private const int MAX_BLOB_COUNT_COUNTING = 100;

        private readonly ConcurrentQueue<RowCountBlob> _rowCountBlobs = new();
        private readonly ICslQueryProvider _queryClient;
        private volatile int _enrichedBlobCount = 0;

        public PreShardingQueueManager(
            string name,
            IEnumerable<BlobEntry> blobList,
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
            : base(name, blobList, credentials, ingestionUri, databaseName, tableName)
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
            var enrichTask = Task.WhenAll(processTasks);

            await ProcessEnrichedBlobsAsync(enrichTask);
            await enrichTask;
        }

        protected override IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported)
        {
            return reported
                .Add("Enriched", _enrichedBlobCount.ToString());
        }

        private async Task ProcessEnrichedBlobsAsync(Task enrichTask)
        {
            var aggregationBuckets = new Dictionary<DateTime, AggregationBucket>();

            while (!enrichTask.IsCompleted || _rowCountBlobs.Any())
            {
                if (_rowCountBlobs.TryDequeue(out var blob))
                {
                    var creationTime = ExtractTimeFromUri(blob.blobEntry.uri);

                    if (!aggregationBuckets.ContainsKey(creationTime))
                    {
                        aggregationBuckets[creationTime] = new AggregationBucket(
                            new List<RowCountBlob>(),
                            0);
                    }

                    var bucket = aggregationBuckets[creationTime];
                    var sizeBefore = bucket.blobs.Sum(i => i.blobEntry.size);
                    var sizeAfter = sizeBefore + blob.blobEntry.size;

                    if (sizeAfter > BATCH_SIZE)
                    {   //  We ingest up to the last whole shard index
                        if (bucket.lastWholeShardLength == 0)
                        {
                            throw new NotSupportedException(
                                "Bigger than batch size blob aren't supported");
                        }
                        await IngestBlobsAsync(
                            creationTime,
                            bucket.blobs
                            .Take(bucket.lastWholeShardLength)
                            .Select(i => i.blobEntry.uri));
                        bucket.blobs.RemoveRange(0, bucket.lastWholeShardLength);
                        bucket = bucket with { lastWholeShardLength = 0 };
                    }

                    var rowsBefore = bucket.blobs.Sum(i => i.rowCount);
                    var rowsAfter = rowsBefore + blob.rowCount;
                    var shardCountBefore = rowsBefore / SHARD_ROW_COUNT;
                    var shardCountAfter = rowsAfter / SHARD_ROW_COUNT;

                    if (shardCountBefore != shardCountAfter)
                    {
                        bucket = bucket with { lastWholeShardLength = bucket.blobs.Count() };
                    }
                    bucket.blobs.Add(blob);
                    aggregationBuckets[creationTime] = bucket;
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

                await IngestBlobsAsync(creationTime, bucket.blobs.Select(i => i.blobEntry.uri));
            }
        }

        private async Task IngestBlobsAsync(DateTime timestamp, IEnumerable<Uri> batchBlobUris)
        {
            var batchId = Guid.NewGuid().ToString();
            var properties = CreateIngestionProperties();

            properties.AdditionalProperties.Add(
                "creationTime",
                $"{timestamp.Year:D2}-{timestamp.Month:D2}-{timestamp.Day:D2} "
                + $"{timestamp.Hour:D2}:00:00.0000");
            properties.DropByTags = new[] { batchId };
            properties.Format = DataSourceFormat.parquet;

            var ingestTasks = batchBlobUris
                 .Select(uri => IngestFromStorageAsync(uri, properties))
                 .ToImmutableArray();

            await Task.WhenAll(ingestTasks);
        }

        private async Task EnrichBlobMetadataAsync()
        {
            while (true)
            {
                var blobEntries = DequeueBlobEntries();

                if (blobEntries.Any())
                {
                    var rowCounts = await FetchParquetRowCountAsync(blobEntries
                        .Select(e => e.uri));
                    var enrichedBlobs = blobEntries
                        .Zip(rowCounts, (entry, rowCount) => new RowCountBlob(
                            entry,
                            rowCount));

                    foreach (var b in enrichedBlobs)
                    {
                        _rowCountBlobs.Enqueue(b);
                    }
                    Interlocked.Add(ref _enrichedBlobCount, enrichedBlobs.Count());
                }
                else
                {
                    return;
                }
            }
        }

        private IImmutableList<BlobEntry> DequeueBlobEntries()
        {
            var list = new List<BlobEntry>(MAX_BLOB_COUNT_COUNTING);

            while (list.Count() < MAX_BLOB_COUNT_COUNTING)
            {
                var blobEntry = DequeueBlobEntry();

                if (blobEntry == null)
                {
                    break;
                }
                else
                {
                    list.Add(blobEntry);
                }
            }

            return list.ToImmutableArray();
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