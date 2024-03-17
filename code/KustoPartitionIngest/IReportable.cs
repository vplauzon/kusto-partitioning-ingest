using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal interface IReportable
    {
        IImmutableDictionary<string, string> GetReport();
    }
}