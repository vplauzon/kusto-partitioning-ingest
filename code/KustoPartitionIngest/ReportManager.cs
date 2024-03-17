
using System.Collections.Immutable;

namespace KustoPartitionIngest
{
    internal class ReportManager
    {
        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(5);

        private readonly IImmutableList<IReportable> _reportables;

        public ReportManager(params IReportable?[] reportables)
        {
            _reportables = reportables
                .Where(r => r != null)
                .ToImmutableArray();
        }

        public async Task RunAsync(Task dependantTask)
        {
            while (!dependantTask.IsCompleted)
            {
                await Task.WhenAny(dependantTask, Task.Delay(PERIOD));

                Report();
            }
            Report();
        }

        private void Report()
        {
            foreach (var reportable in _reportables)
            {
                var report = reportable.GetReport();
                var values = report
                    .Select(p => $"{p.Key} ({p.Value})");
                var text = string.Join(", ", values);

                Console.WriteLine($"{reportable.Name}:  {text}");
            }
        }
    }
}