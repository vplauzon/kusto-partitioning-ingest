
using System.Collections.Immutable;

namespace KustoPartitionIngest
{
    internal interface IQueueManager : IReportable
    {
        Task RunAsync();
    }
}