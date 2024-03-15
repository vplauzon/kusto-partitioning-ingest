using Azure.Core;
using Kusto.Data;
using Kusto.Ingest;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoPartitionIngest.Partitioning
{
    internal class PartitioningQueueManager : QueueManagerBase
    {
        private const int PARALLEL_QUEUING = 32;

        private readonly bool _hasPartitioningHint;
        private readonly string _partitionKeyColumn;

        public PartitioningQueueManager(
            TokenCredential credentials,
            string ingestionUri,
            string databaseName,
            string tableName,
            bool hasPartitioningHint,
            string partitionKeyColumn)
            : base(credentials, ingestionUri, databaseName, tableName)
        {
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

        private async Task ProcessUriAsync()
        {
            Uri? blobUri;

            while ((blobUri = await DequeueBlobUriAsync()) != null)
            {
                (var timestamp, var partitionKey) = AnalyzeUri(blobUri);
                var properties = CreateIngestionProperties();

                if (timestamp != null)
                {
                    properties.AdditionalProperties.Add(
                        "creationTime",
                        $"{timestamp.Value.Year}-{timestamp.Value.Month}-{timestamp.Value.Day}");
                }
                if (partitionKey != null && _hasPartitioningHint)
                {
                    properties.AdditionalProperties.Add(
                        "dataPartitionValueHint",
                        $"{{'{_partitionKeyColumn}':'{partitionKey}'}}");
                }

                await IngestFromStorageAsync(blobUri, properties);
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