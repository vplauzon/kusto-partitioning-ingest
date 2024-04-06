using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class RefCounterSignaler : ISignaler
    {
        private volatile int _counter = 0;
        private volatile TaskCompletionSource? _taskSource = null;

        #region ISignaler
        void ISignaler.Signal()
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
            _taskSource = new TaskCompletionSource();

            await _taskSource.Task;
        }
    }
}