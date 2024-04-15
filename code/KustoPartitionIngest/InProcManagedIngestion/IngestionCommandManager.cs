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
        private record CommandQueueItem(string commandText, ICompleter completer);
        #endregion

        private readonly ICslAdminProvider _commandClient;
        private readonly string _databaseName;
        private readonly Task<long> _capacityTask;
        private readonly OperationManager _operationManager;
        private readonly ConcurrentQueue<CommandQueueItem> _commandQueue = new();
        private readonly ConcurrentQueue<Task> _ingestionTaskQueue = new();
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
            while (_commandQueue.Any() || _ingestionTaskQueue.Any())
            {
                if (_ingestionTaskQueue.TryDequeue(out var task))
                {
                    await task;
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
            await using (_operationManager)
            {
            }
        }
        #endregion

        public async Task QueueIngestionAsync(ICompleter completer, string commandText)
        {
            _commandQueue.Enqueue(new CommandQueueItem(commandText, completer));
            await TryScheduleIngestionAsync();
        }

        private async Task TryScheduleIngestionAsync()
        {
            var capacity = await _capacityTask;
            var newCommandCount = Interlocked.Increment(ref _commandCount);

            if (newCommandCount <= capacity)
            {
                await PushIngestionsAsync();
            }
        }

        private async Task PushIngestionsAsync()
        {
            if (_commandQueue.TryDequeue(out var commandItem))
            {
                var reader = await _commandClient.ExecuteControlCommandAsync(
                    _databaseName,
                    commandItem.commandText);
                var table = reader.ToDataSet().Tables[0];
                var operationId = ((Guid)(table.Rows[0][0])).ToString();
                var decoratedCompleter = new ActionCompleter(
                    () =>
                    {
                        _ingestionTaskQueue.Enqueue(PushIngestionsAsync());
                        commandItem.completer.Complete();
                    },
                    ex => commandItem.completer.SetException(ex));

                _operationManager.TrackOperationCompleted(decoratedCompleter, operationId);
            }
            else
            {
                Interlocked.Decrement(ref _commandCount);
            }
        }
    }
}