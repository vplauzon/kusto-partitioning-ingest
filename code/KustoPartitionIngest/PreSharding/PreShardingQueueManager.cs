using Azure.Core;
using Azure.Storage.Queues;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;

namespace KustoPartitionIngest.PreSharding
{
    internal class PreShardingQueueManager : SparkCreationTimeQueueManagerBase, IAsyncDisposable
    {
        #region Inner types
        private record RowCountBlob(BlobEntry blobEntry, long rowCount);

        private record AggregationBucket(List<RowCountBlob> blobs, int lastWholeShardLength);
        #endregion

        private const long SHARD_ROW_COUNT = 1048576;
        private const long BATCH_SIZE = 1000 * 1000 * 1000 / 8;
        private const int PARALLEL_COUNTING = 15;
        private const int MAX_BLOB_COUNT_COUNTING = 100;

        private readonly ConcurrentQueue<RowCountBlob> _rowCountBlobs = new();
        private readonly ICslQueryProvider _queryClient;
        private readonly string _databaseName;
        private readonly InProcIngestionManager _ingestionManager;
        private volatile int _enrichedBlobCount = 0;
        private volatile int _queueCount = 0;
        private volatile int _ingestionCount = 0;

        public PreShardingQueueManager(
            string name,
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            InProcIngestionManager ingestionManager,
            IEnumerable<BlobEntry> blobList)
            : base(name, blobList)
        {
            var queryUri = InProcIngestionManager.GetQueryUriFromIngestionUri(ingestionUri);
            var connectionBuilder = new KustoConnectionStringBuilder(queryUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);
            var queryClient = KustoClientFactory.CreateCslQueryProvider(connectionBuilder);

            _queryClient = queryClient;
            _databaseName = databaseName;
            _ingestionManager = ingestionManager;
        }

        #region IAsyncDisposable
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await using (_ingestionManager)
            {
            }
            using (_queryClient)
            {
            }
        }
        #endregion

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
                .Add("Enriched", _enrichedBlobCount.ToString())
                .Add("Queued", _queueCount.ToString())
                .Add("Ingested", _ingestionCount.ToString());
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
                        await QueueIngestionAsync(
                            bucket.blobs
                            .Take(bucket.lastWholeShardLength)
                            .Select(i => i.blobEntry.uri),
                            creationTime);

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

                await QueueIngestionAsync(
                    bucket.blobs.Select(i => i.blobEntry.uri),
                    creationTime);
            }
        }

        private async Task QueueIngestionAsync(IEnumerable<Uri> blobUris, DateTime creationTime)
        {
            var blobCount = blobUris.Count();
            var completer = new ActionCompleter(() =>
            {
                Interlocked.Add(ref _ingestionCount, blobCount);
            });

            await _ingestionManager.QueueIngestionAsync(completer, blobUris, creationTime);
            Interlocked.Add(ref _queueCount, blobCount);
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
                _databaseName,
                query,
                new ClientRequestProperties());
            var counts = reader.ToDataSet().Tables[0].Rows[0].ItemArray
                .Cast<long>();

            return counts;
        }
    }
}