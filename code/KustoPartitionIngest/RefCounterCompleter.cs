using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class RefCounterCompleter : ICompleter
    {
        private volatile int _counter = 0;
        private volatile TaskCompletionSource? _taskSource = null;

        #region ICompleter
        void ICompleter.Complete()
        {
            if (Interlocked.Decrement(ref _counter) == 0
                && _taskSource != null)
            {
                _taskSource.SetResult();
            }
        }
        #endregion

        public void IncrementCounter()
        {
            Interlocked.Increment(ref _counter);
        }

        public async Task AwaitCompletionAsync()
        {
            if (_counter != 0)
            {
                _taskSource = new TaskCompletionSource();

                await _taskSource.Task;
            }
        }
    }
}