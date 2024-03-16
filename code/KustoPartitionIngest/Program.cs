using Azure.Identity;
using KustoPartitionIngest.Partitioning;
using KustoPartitionIngest.PreSharding;

namespace KustoPartitionIngest
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length > 0)
            {
                var firstParam = args[0];

                if (firstParam.Length == 2 && firstParam[0] == '-')
                {
                    var flow = firstParam[1];

                    switch (flow)
                    {
                        case 'p':
                            await RunPartitioningFlowAsync(args);

                            return;
                        case 's':
                            await RunPreShardingFlowAsync(args);

                            return;
                        default:
                            throw new NotSupportedException($"Flow '{flow}' isn't supported");
                    }
                }
            }
            DisplayHelp();
        }

        private static async Task RunPartitioningFlowAsync(string[] args)
        {
            if (args.Length >= 6)
            {
                var storageUrl = args[1];
                var nonSasStorageUrl = storageUrl.Split('?').First();
                var databaseName = args[2];
                var tableName = args[3];
                var partitionKeyColumn = args[4];
                var ingestionUri1 = args[5];
                var ingestionUri2 = args.Length >= 7 ? args[6] : string.Empty;
                var credentials = new DefaultAzureCredential(true);
                var queueManager1 = new PartitioningQueueManager(
                    credentials,
                    new Uri(ingestionUri1),
                    databaseName,
                    tableName,
                    true,
                    partitionKeyColumn);
                var queueManager2 = string.IsNullOrWhiteSpace(ingestionUri2)
                    ? null
                    : new PartitioningQueueManager(
                        credentials,
                        new Uri(ingestionUri2),
                        databaseName,
                        tableName,
                        false,
                        partitionKeyColumn);
                var orchestrator = new BulkOrchestrator(
                    queueManager1,
                    queueManager2,
                    storageUrl);

                Console.WriteLine($"Storage URL:  {nonSasStorageUrl}");
                Console.WriteLine($"Kusto Database Name:  {databaseName}");
                Console.WriteLine($"Kusto Table Name:  {tableName}");
                Console.WriteLine($"Partition Key Column:  {partitionKeyColumn}");
                Console.WriteLine($"Ingestion URI 1 (with hint):  {ingestionUri1}");
                Console.WriteLine($"Ingestion URI 2 (without hint):  {ingestionUri2}");

                await orchestrator.RunAsync();
            }
            else
            {
                DisplayHelp();
            }
        }

        private static async Task RunPreShardingFlowAsync(string[] args)
        {
            if (args.Length >= 5)
            {
                var storageUrl = args[1];
                var nonSasStorageUrl = storageUrl.Split('?').First();
                var databaseName = args[2];
                var tableName = args[3];
                var ingestionUri1 = args[4];
                var ingestionUri2 = args.Length >= 6 ? args[5] : string.Empty;
                var credentials = new DefaultAzureCredential(true);
                var queueManager1 = new PreShardingQueueManager(
                    credentials,
                    new Uri(ingestionUri1),
                    databaseName,
                    tableName);
                var orchestrator = new BulkOrchestrator(
                    queueManager1,
                    null,
                    storageUrl);

                Console.WriteLine($"Storage URL:  {nonSasStorageUrl}");
                Console.WriteLine($"Kusto Database Name:  {databaseName}");
                Console.WriteLine($"Kusto Table Name:  {tableName}");
                Console.WriteLine($"Ingestion URI 1 (with hint):  {ingestionUri1}");
                Console.WriteLine($"Ingestion URI 2 (without hint):  {ingestionUri2}");

                await orchestrator.RunAsync();
            }
            else
            {
                DisplayHelp();
            }
        }

        private static void DisplayHelp()
        {
            Console.Error.WriteLine("Expected CLI parameters for partitioning:");
            Console.Error.WriteLine("* '-p'");
            Console.Error.WriteLine("* Storage root folder");
            Console.Error.WriteLine("* Kusto Database Name");
            Console.Error.WriteLine("* Kusto Table Name");
            Console.Error.WriteLine("* Partition Key Column Name");
            Console.Error.WriteLine("* Kusto Ingestion URI");
            Console.Error.WriteLine();
            Console.Error.WriteLine("At least one Kusto Ingestion URI is expected");
            Console.Error.WriteLine("Partitioning hint is passed on the first one only");

            Console.Error.WriteLine();
            Console.Error.WriteLine("Expected CLI parameters for pre-sharding:");
            Console.Error.WriteLine("* '-s'");
            Console.Error.WriteLine("* Storage root folder");
            Console.Error.WriteLine("* Kusto Database Name");
            Console.Error.WriteLine("* Kusto Table Name");
            Console.Error.WriteLine("* Kusto Ingestion URI");
            Console.Error.WriteLine();
            Console.Error.WriteLine("At least one Kusto Ingestion URI is expected");
            Console.Error.WriteLine("Pre-sharding is done on the first one only");
        }
    }
}