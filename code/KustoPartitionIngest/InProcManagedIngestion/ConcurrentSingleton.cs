using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest.InProcManagedIngestion
{
    internal class ConcurrentSingleton
    {
        private volatile int _witness = 0;

        public bool IsActive()
        {
            return _witness != 0;
        }

        public bool TryActivate()
        {
            return Interlocked.CompareExchange(ref _witness, 1, 0) == 0;
        }

        public void Deactivate()
        {
            Interlocked.CompareExchange(ref _witness, 0, 1);
        }
    }
}