namespace KustoPartitionIngest
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 5)
            {
                Console.Error.WriteLine("Expected CLI parameters:");
                Console.Error.WriteLine("* Storage root folder");
                Console.Error.WriteLine("* Kusto Database Name");
                Console.Error.WriteLine("* Kusto Table Name");
                Console.Error.WriteLine("* Partition Key Column Name");
                Console.Error.WriteLine("* Kusto Ingestion URI");
                Console.Error.WriteLine();
                Console.Error.WriteLine("At least one Kusto Ingestion URI is expected");
                Console.Error.WriteLine("If one is provided, partitioning hint are passed "
                    + "during ingestion");
                Console.Error.WriteLine("If two are provided, the second one doesn't get"
                    + "partitioning hint");
            }
            else
            {
                var storageUrl = args[0];
                var databaseName = args[1];
                var tableName = args[2];
                var partitionKeyColumn = args[3];
                var ingestionUri1 = args[4];
                var ingestionUri2 = args.Length >= 6 ? args[5] : string.Empty;
                var orchestrator = new BulkOrchestrator(
                    storageUrl,
                    databaseName,
                    tableName,
                    partitionKeyColumn,
                    ingestionUri1,
                    ingestionUri2);

                Console.WriteLine($"Storage URL:  {storageUrl}");
                Console.WriteLine($"Kusto Database Name:  {databaseName}");
                Console.WriteLine($"Kusto Table Name:  {tableName}");
                Console.WriteLine($"Partition Key Column:  {partitionKeyColumn}");
                Console.WriteLine($"Ingestion URI 1:  {ingestionUri1}");
                Console.WriteLine($"Ingestion URI 2:  {ingestionUri2}");

                await orchestrator.RunAsync();
            }
        }
    }
}