using Azure.Core;
using Kusto.Cloud.Platform.Msal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest.PreSharding
{
    internal abstract class SparkCreationTimeQueueManagerBase : QueueManagerBase
    {
        protected SparkCreationTimeQueueManagerBase(
            TokenCredential credentials,
            Uri ingestionUri,
            string databaseName,
            string tableName)
            : base(credentials, ingestionUri, databaseName, tableName)
        {
        }

        protected DateTime ExtractTimeFromUri(Uri blobUri)
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
    }
}