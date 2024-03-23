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

        private record OperationQueueItem(
            string operationId,
            Task operationTask,
            TaskCompletionSource commandSource);
        #endregion

        private readonly ICslAdminProvider _commandClient;
        private readonly string _databaseName;
        private readonly long _capacity;
        private readonly OperationManager _operationManager;
        private readonly ConcurrentQueue<CommandQueueItem> _commandQueue = new();
        private readonly ConcurrentSingleton _managementSingleton = new();
        private Task _managementTask = Task.CompletedTask;

        #region Constructor
        public static async Task<IngestionCommandManager> CreateAsync(
            ICslAdminProvider commandClient,
            string databaseName)
        {
            var reader = await commandClient.ExecuteControlCommandAsync(
                databaseName,
                ".show capacity ingestions | project Total");
            var capacity = (long)(reader.ToDataSet().Tables[0].Rows[0].ItemArray[0]!);

            return new IngestionCommandManager(
                commandClient,
                databaseName,
                capacity);
        }

        private IngestionCommandManager(
            ICslAdminProvider commandClient,
            string databaseName,
            long capacity)
        {
            _commandClient = commandClient;
            _databaseName = databaseName;
            _capacity = capacity;
            _operationManager = new OperationManager(commandClient, databaseName);
        }
        #endregion

        #region IAsyncDisposable
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _managementTask;
            await using (_operationManager)
            {
            }
        }
        #endregion

        public async Task ExecuteIngestionAsync(string commandText)
        {
            var source = new TaskCompletionSource();

            _commandQueue.Enqueue(new CommandQueueItem(commandText, source));
            if (_managementSingleton.TryActivate())
            {
                await _managementTask;
                _managementTask = ManageQueueAsync();
            }

            await source.Task;
        }

        private async Task ManageQueueAsync()
        {
            var ingestionList = new List<OperationQueueItem>();

            do
            {
                await PushIngestionsAsync(ingestionList);
                await DetectIngestionCompletionAsync(ingestionList);
            }
            while (ingestionList.Any() || _commandQueue.Any());
            _managementSingleton.Deactivate();
            //  Here we fight the racing condition that something might have
            //  been added to the queue while we were deactivating
            if (_commandQueue.Any())
            {
                if (_managementSingleton.TryActivate())
                {   //  This thread is the winner and we simply continue to process
                    await ManageQueueAsync();
                }
            }
        }

        private static async Task DetectIngestionCompletionAsync(
            List<OperationQueueItem> ingestionList)
        {
            await Task.WhenAny(ingestionList.Select(o => o.operationTask));

            var snapshots = ingestionList
                .Select(o => new { Item = o, IsCompleted = o.operationTask.IsCompleted })
                .ToImmutableArray();
            var completedItems = snapshots.Where(i => i.IsCompleted).Select(i => i.Item);
            var incompletedItems = snapshots.Where(i => !i.IsCompleted).Select(i => i.Item);

            await Task.WhenAll(completedItems.Select(i => i.operationTask));
            foreach (var i in completedItems)
            {
                i.commandSource.SetResult();
            }
            ingestionList.Clear();
            ingestionList.AddRange(incompletedItems);
        }

        private async Task PushIngestionsAsync(List<OperationQueueItem> ingestionList)
        {
            while (ingestionList.Count() < _capacity
                && _commandQueue.TryDequeue(out var commandItem))
            {
                var operationId = await PushIngestionAsync(commandItem.commandText);
                var operationTask = _operationManager.OperationCompletedAsync(operationId);

                ingestionList.Add(new OperationQueueItem(
                    operationId,
                    operationTask,
                    commandItem.source));
            }
        }

        private async Task<string> PushIngestionAsync(string commandText)
        {
            var reader =
                await _commandClient.ExecuteControlCommandAsync(_databaseName, commandText);
            var operationId = (string)(reader.ToDataSet().Tables[0].Rows[0][0]);

            return operationId;
        }
    }
}