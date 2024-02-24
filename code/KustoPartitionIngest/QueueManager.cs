using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using Kusto.Ingest;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoPartitionIngest
{
    internal class QueueManager
    {
        private const int PARALLEL_QUEUING = 32;

        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly bool _hasPartitioningHint;
        private readonly string _databaseName;
        private readonly string _tableName;
        private readonly string _partitionKeyColumn;
        private readonly ConcurrentQueue<Uri> _blobUris = new();
        private bool _isCompleted = false;

        public event EventHandler? BlobUriQueued;

        public QueueManager(
            TokenCredential credentials,
            bool hasPartitioningHint,
            string ingestionUri,
            string databaseName,
            string tableName,
            string partitionKeyColumn)
        {
            var builder = new KustoConnectionStringBuilder(ingestionUri)
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            _ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);
            _hasPartitioningHint = hasPartitioningHint;
            _databaseName = databaseName;
            _tableName = tableName;
            _partitionKeyColumn = partitionKeyColumn;
        }

        public void QueueUri(Uri blobUri)
        {
            _blobUris.Enqueue(blobUri);
        }

        public void Complete()
        {
            _isCompleted = true;
        }

        public async Task RunAsync()
        {
            var processTasks = Enumerable.Range(0, PARALLEL_QUEUING)
                .Select(i => Task.Run(() => ProcessUriAsync()))
                .ToImmutableArray();

            await Task.WhenAll(processTasks);
        }

        private async Task ProcessUriAsync()
        {
            while (!_isCompleted || _blobUris.Any())
            {
                if (_blobUris.TryDequeue(out var blobUri))
                {
                    (var timestamp, var partitionKey) = AnalyzeUri(blobUri);
                    var properties = new KustoIngestionProperties(_databaseName, _tableName);

                    if (timestamp != null)
                    {
                        properties.AdditionalProperties.Add(
                            "creationTime",
                            $"{timestamp.Value.Year}-{timestamp.Value.Month}-{timestamp.Value.Day}");
                    }
                    if (partitionKey != null)
                    {
                        properties.AdditionalProperties.Add(
                            "dataPartitionValueHint",
                            $"{{'{_partitionKeyColumn}':'{partitionKey}'}}");
                    }

                    await _ingestClient.IngestFromStorageAsync($"{blobUri}", properties);
                    RaiseBlobUriQueued();
                }
                else
                {   //  Wait for more blobs
                    await Task.Delay(TimeSpan.FromSeconds(1));
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

        private void RaiseBlobUriQueued()
        {
            if (BlobUriQueued != null)
            {
                BlobUriQueued(this, EventArgs.Empty);
            }
        }
    }
}