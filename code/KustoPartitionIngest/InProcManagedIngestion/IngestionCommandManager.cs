using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest.InProcManagedIngestion
{
    internal class IngestionCommandManager : IAsyncDisposable
    {
        #region Inner types
        private record CommandQueueItem(string commandText, TaskCompletionSource source);

        private record OperationQueueItem(string operationId, Task operationTask);
        #endregion

        private readonly ICslAdminProvider _commandClient;
        private readonly string _databaseName;
        private readonly Task<long> _capacityTask;
        private readonly OperationManager _operationManager;
        private readonly ConcurrentQueue<CommandQueueItem> _commandQueue = new();
        private readonly ConcurrentQueue<OperationQueueItem> _operationQueue = new();
        private volatile int _commandCount = 0;

        #region Constructor
        public IngestionCommandManager(
            ICslAdminProvider commandClient,
            string databaseName)
        {
            _commandClient = commandClient;
            _databaseName = databaseName;
            _capacityTask = FetchCapacityAsync(commandClient, databaseName);
            _operationManager = new OperationManager(commandClient, databaseName);
        }

        private static async Task<long> FetchCapacityAsync(
            ICslAdminProvider commandClient,
            string databaseName)
        {
            var reader = await commandClient.ExecuteControlCommandAsync(
                databaseName,
                ".show capacity ingestions | project Total");
            var capacity = (long)(reader.ToDataSet().Tables[0].Rows[0].ItemArray[0]!);

            return capacity;
        }
        #endregion

        #region IAsyncDisposable
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _capacityTask;
            await Task.WhenAll(_commandQueue.Select(i => i.source.Task));
            await Task.WhenAll(_operationQueue.Select(i => i.operationTask));
            await using (_operationManager)
            {
            }
        }
        #endregion

        public async Task ExecuteIngestionAsync(string commandText)
        {
            var source = new TaskCompletionSource();

            _commandQueue.Enqueue(new CommandQueueItem(commandText, source));
            await TryScheduleIngestionAsync();
            await source.Task;
        }

        private async Task TryScheduleIngestionAsync()
        {
            var capacity = await _capacityTask;
            var newCommandCount = Interlocked.Increment(ref _commandCount);

            if (newCommandCount <= capacity)
            {
                var operationQueueItem = await PushIngestionsAsync();

                if (operationQueueItem != null)
                {
                    _operationQueue.Enqueue(operationQueueItem);

                    return;
                }
            }
            Interlocked.Decrement(ref _commandCount);
        }

        private async Task<OperationQueueItem?> PushIngestionsAsync()
        {
            if (_commandQueue.TryDequeue(out var commandItem))
            {
                var reader = await _commandClient.ExecuteControlCommandAsync(
                    _databaseName,
                    commandItem.commandText);
                var table = reader.ToDataSet().Tables[0];
                var operationId = ((Guid)(table.Rows[0][0])).ToString();
                var operationTask = _operationManager.OperationCompletedAsync(operationId);
                var postOperationTask = operationTask.ContinueWith(async (t) =>
                {
                    if (t.IsFaulted && t.Exception != null)
                    {
                        commandItem.source.SetException(t.Exception);
                    }
                    else
                    {
                        commandItem.source.SetResult();
                        await TryScheduleIngestionAsync();
                    }
                });

                return new OperationQueueItem(operationId, postOperationTask);
            }
            else
            {
                return null;
            }
        }
    }
}