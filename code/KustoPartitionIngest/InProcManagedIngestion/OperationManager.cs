using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest.InProcManagedIngestion
{
    internal class OperationManager : IAsyncDisposable
    {
        #region Inner types
        private record OperationItem(string operationId, TaskCompletionSource source);
        #endregion

        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(1);

        private readonly ICslAdminProvider _commandClient;
        private readonly string _databaseName;
        private readonly ConcurrentQueue<OperationItem> _operationQueue = new();
        private readonly ConcurrentSingleton _managementSingleton = new();
        private Task _managementTask = Task.CompletedTask;

        public OperationManager(ICslAdminProvider commandClient, string databaseName)
        {
            _commandClient = commandClient;
            _databaseName = databaseName;
        }

        #region IAsyncDisposable
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _managementTask;
        }
        #endregion

        public async Task OperationCompletedAsync(string operationId)
        {
            var source = new TaskCompletionSource();

            _operationQueue.Enqueue(new OperationItem(operationId, source));
            if (_managementSingleton.TryActivate())
            {
                await _managementTask;
                _managementTask = ManageQueueAsync();
            }

            await source.Task;
        }

        private async Task ManageQueueAsync()
        {
            var operationMap = new Dictionary<string, OperationItem>();

            do
            {
                TransferOperations(_operationQueue, operationMap);
                await Task.Delay(PERIOD);
                await DetectOperationCompletionAsync(operationMap);
            }
            while (operationMap.Any() || _operationQueue.Any());
            _managementSingleton.Deactivate();
            //  Here we fight the racing condition that something might have
            //  been added to the queue while we were deactivating
            if (_operationQueue.Any())
            {
                if (_managementSingleton.TryActivate())
                {   //  This thread is the winner and we simply continue to process
                    await ManageQueueAsync();
                }
            }
        }

        private async Task DetectOperationCompletionAsync(
            IDictionary<string, OperationItem> operationMap)
        {
            var operationIdList = string.Join(", ", operationMap.Keys);
            var commandText = $".show operations ({operationIdList})";
            var reader = await _commandClient.ExecuteControlCommandAsync(_databaseName, commandText);
            var results = reader.ToDataSet().Tables[0].Rows.Cast<DataRow>()
                .Select(r => new
                {
                    OperationId = (string)r["OperationId"],
                    State = (string)r["State"],
                    Status = (string)r["Status"]
                })
                .ToImmutableArray();

            foreach (var result in results)
            {
                switch (result.State)
                {
                    case "Completed":
                        operationMap[result.OperationId].source.SetResult();
                        operationMap.Remove(result.OperationId);
                        break;
                    case "Failed":
                    case "PartiallySucceeded":
                    case "Abandoned":
                    case "BadInput":
                    case "Throttled":
                    case "Canceled":
                    case "Skipped":
                        operationMap[result.OperationId].source.SetException(
                            new InvalidOperationException(
                                $"Operation failed:  '{result.Status}'"));
                        break;
                }
            }

            throw new NotImplementedException();
        }

        private void TransferOperations(
            ConcurrentQueue<OperationItem> operationQueue,
            IDictionary<string, OperationItem> operationMap)
        {
            while (operationQueue.TryDequeue(out var item))
            {
                operationMap.Add(item.operationId, item);
            }
        }
    }
}