﻿using Azure.Core;
using System.Collections.Immutable;

namespace KustoPartitionIngest.Partitioning
{
    internal class PartitioningQueueManager : QueueManagerBase
    {
        private const int PARALLEL_QUEUING = 32;
        private readonly DmBackedIngestionManager _ingestionManager;
        private readonly bool _hasPartitioningHint;
        private readonly string _partitionKeyColumn;

        public PartitioningQueueManager(
            string name,
            IEnumerable<BlobEntry> blobList,
            DmBackedIngestionManager ingestionManager,
            bool hasPartitioningHint,
            string partitionKeyColumn)
            : base(name, blobList)
        {
            _ingestionManager = ingestionManager;
            _hasPartitioningHint = hasPartitioningHint;
            _partitionKeyColumn = partitionKeyColumn;
        }

        protected override async Task RunInternalAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_QUEUING)
                .Select(i => Task.Run(() => ProcessUriAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        protected override IImmutableDictionary<string, string> AlterReported(
            IImmutableDictionary<string, string> reported)
        {
            return reported
                .Add("Queued", _ingestionManager.QueueCount.ToString());
        }

        private async Task ProcessUriAsync()
        {
            BlobEntry? blobEntry;

            while ((blobEntry = DequeueBlobEntry()) != null)
            {
                (var timestamp, var partitionKey) = AnalyzeUri(blobEntry.uri);

                if (partitionKey != null && _hasPartitioningHint)
                {
                    await _ingestionManager.QueueIngestionAsync(
                        new[] { blobEntry.uri },
                        timestamp,
                        (_partitionKeyColumn, partitionKey));
                }
                else
                {
                    await _ingestionManager.QueueIngestionAsync(
                        new[] { blobEntry.uri },
                        timestamp);
                }
            }
        }

        private (DateTime? timestamp, string? partitionKey) AnalyzeUri(Uri blobUri)
        {
            var parts = blobUri.AbsolutePath.Split('/');
            var partitions = parts.TakeLast(5).Take(4);

            if (partitions.Count() == 4)
            {
                var year = GetInteger(partitions.First());
                var month = GetInteger(partitions.Skip(1).First());
                var day = GetInteger(partitions.Skip(2).First());
                var timestamp = GetTimestamp(year, month, day);
                var partitionKey = partitions.Last();

                return (timestamp, partitionKey);
            }
            else
            {
                return (null, null);
            }

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

            DateTime? GetTimestamp(int? year, int? month, int? day)
            {
                if (year != null && month != null && day != null)
                {
                    try
                    {
                        return new DateTime(year.Value, month.Value, day.Value);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        return null;
                    }
                }
                else
                {
                    return null;
                }
            }
        }
    }
}