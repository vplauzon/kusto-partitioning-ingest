using Azure.Core;
using Azure.Storage.Queues;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Net.Sockets;
using System.Reflection.Metadata;
using static System.Reflection.Metadata.BlobBuilder;

namespace KustoPartitionIngest.PreSharding
{
    internal class PreShardingQueueManager : SparkCreationTimeQueueManagerBase, IAsyncDisposable
    {
        #region Inner types
        private record RowCountBlob(BlobEntry blobEntry, long rowCount);

        private record BlobUriBatch(DateTime creationTime, IEnumerable<Uri> blobUris);

        private class BlobBatching : IEnumerable<BlobUriBatch>
        {
            private readonly IEnumerable<RowCountBlob> _rowCountBlobs;
            private readonly Func<Uri, DateTime> _extractTimeFromUri;

            public BlobBatching(
                IEnumerable<RowCountBlob> rowCountBlobs,
                Func<Uri, DateTime> extractTimeFromUri)
            {
                _rowCountBlobs = rowCountBlobs;
                _extractTimeFromUri = extractTimeFromUri;
            }

            #region IEnumerable<BlobUriBatch>
            IEnumerator<BlobUriBatch> IEnumerable<BlobUriBatch>.GetEnumerator()
            {
                var blobsByCreationTime = _rowCountBlobs
                    .GroupBy(b => _extractTimeFromUri(b.blobEntry.uri))
                    .ToImmutableDictionary(g => g.Key);

                foreach (var creationTime in blobsByCreationTime.Keys.OrderBy(k => k))
                {   //  Sort blobs by decreasing amount of rows
                    var blobs = blobsByCreationTime[creationTime]
                        .OrderByDescending(b => b.rowCount)
                        .ToList();

                    //  Run until we empty the time partition
                    while (blobs.Any())
                    {
                        var batch = GetBatch(blobs);
                        var paddedBatch = GetPaddedBatch(batch, blobs);
                        var uris = paddedBatch
                            .Select(b => b.blobEntry.uri);

                        yield return new BlobUriBatch(creationTime, uris);
                    }
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return ((IEnumerable<BlobUriBatch>)this).GetEnumerator();
            }
            #endregion

            private IImmutableList<RowCountBlob> GetBatch(List<RowCountBlob> blobs)
            {
                var blobIndex = new List<int>();
                var i = 0;
                var lastWholeShardIndexLength = 0;
                long totalSize = 0;
                long totalRows = 0;

                while (i < blobs.Count)
                {
                    if (blobs[i].blobEntry.size + totalSize <= BATCH_SIZE)
                    {
                        var rowsBefore = totalRows;
                        var rowsAfter = totalRows + blobs[i].rowCount;
                        var shardCountBefore = rowsBefore / SHARD_ROW_COUNT;
                        var shardCountAfter = rowsAfter / SHARD_ROW_COUNT;

                        if (shardCountBefore != shardCountAfter)
                        {
                            lastWholeShardIndexLength = blobIndex.Count;
                        }
                        totalRows = rowsAfter;
                        totalSize += blobs[i].blobEntry.size;
                        blobIndex.Add(i);
                        ++i;
                    }
                    else
                    {
                        return PickIndexes(blobs, blobIndex.Take(lastWholeShardIndexLength));
                    }
                }

                //  Reach this point iif we emptied the blob list
                return PickIndexes(blobs, blobIndex);
            }

            //  Here we try to pad a batch with smaller blobs while staying with the same number
            //  of shards
            private IImmutableList<RowCountBlob> GetPaddedBatch(
                IImmutableList<RowCountBlob> batch,
                List<RowCountBlob> blobs)
            {
                var blobIndex = new List<int>();
                var i = 0;
                var totalRows = batch.Sum(b => b.rowCount);
                var shardCount = totalRows / SHARD_ROW_COUNT;

                while (i < blobs.Count)
                {
                    var newTotalRows = totalRows + blobs[i].rowCount;
                    var newShardCount = newTotalRows / SHARD_ROW_COUNT;

                    if (newShardCount == shardCount)
                    {
                        totalRows = newTotalRows;
                        blobIndex.Add(i);
                    }
                    ++i;
                }

                //  Reach this point iif we emptied the blob list
                return batch
                    .AddRange(PickIndexes(blobs, blobIndex));
            }

            private IImmutableList<RowCountBlob> PickIndexes(
                List<RowCountBlob> blobs,
                IEnumerable<int> indexes)
            {
                var subBlobs = indexes
                    .Select(i => blobs[i])
                    .ToImmutableArray();

                foreach (var i in indexes.OrderByDescending(i => i))
                {
                    blobs.RemoveAt(i);
                }

                return subBlobs;
            }
        }
        #endregion

        private const long SHARD_ROW_COUNT = 1048576;
        private const long BATCH_SIZE = 1000 * 1000 * 1000 / 8;
        private const int PARALLEL_COUNTING = 15;
        private const int BLOB_COUNT_COUNTING_BATCH = 100;

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

            await Task.WhenAll(processTasks);

            var enrichedBlobs = processTasks
                .Select(t => t.Result)
                .SelectMany(i => i)
                .SelectMany(i => i)
                .ToImmutableArray();

            await IngestEnrichedBlobsAsync(enrichedBlobs);
        }

        protected override IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported)
        {
            return reported
                .Add("Enriched", _enrichedBlobCount.ToString())
                .Add("Queued", _queueCount.ToString())
                .Add("Ingested", _ingestionCount.ToString());
        }

        private async Task IngestEnrichedBlobsAsync(IImmutableList<RowCountBlob> enrichedBlobs)
        {
            var ingestionQueueCount = 0;
            var ingestionDoneCount = 0;
            TaskCompletionSource? ingestionDoneTaskSource = null;
            var countCompleter = new ActionCompleter(() =>
            {
                Interlocked.Increment(ref ingestionDoneCount);
                if (ingestionDoneTaskSource != null && ingestionQueueCount == ingestionDoneCount)
                {
                    ingestionDoneTaskSource.SetResult();
                }
            });
            var blobBatching = new BlobBatching(enrichedBlobs, ExtractTimeFromUri);

            foreach (var batch in blobBatching)
            {
                await QueueIngestionAsync(countCompleter, batch.blobUris, batch.creationTime);
            }

            ingestionDoneTaskSource = new TaskCompletionSource();
            await ingestionDoneTaskSource.Task;
        }

        private async Task QueueIngestionAsync(
            ICompleter completer,
            IEnumerable<Uri> blobUris, DateTime creationTime)
        {
            var blobCount = blobUris.Count();
            var decoratedCompleter = new ActionCompleter(() =>
            {
                Interlocked.Add(ref _ingestionCount, blobCount);
                completer.Complete();
            },
            ex => completer.SetException(ex));

            await _ingestionManager.QueueIngestionAsync(
                decoratedCompleter, blobUris, creationTime);
            Interlocked.Add(ref _queueCount, blobCount);
        }

        private async Task<IEnumerable<IEnumerable<RowCountBlob>>> EnrichBlobMetadataAsync()
        {
            var blobHeap = new List<IEnumerable<RowCountBlob>>();

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

                    blobHeap.Add(enrichedBlobs);
                    Interlocked.Add(ref _enrichedBlobCount, enrichedBlobs.Count());
                }
                else
                {
                    return blobHeap;
                }
            }
        }

        private IImmutableList<BlobEntry> DequeueBlobEntries()
        {
            var list = new List<BlobEntry>(BLOB_COUNT_COUNTING_BATCH);

            while (list.Count() < BLOB_COUNT_COUNTING_BATCH)
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