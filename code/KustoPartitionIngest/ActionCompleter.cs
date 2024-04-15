using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    internal class ActionCompleter : ICompleter
    {
        private readonly Action _completeAction;
        private readonly Action<Exception>? _exceptionAction;

        public ActionCompleter(Action completeAction, Action<Exception>? exceptionAction = null)
        {
            _completeAction = completeAction;
            _exceptionAction = exceptionAction;
        }

        void ICompleter.Complete()
        {
            _completeAction();
        }

        void ICompleter.SetException(Exception ex)
        {
            if (_exceptionAction != null)
            {
                _exceptionAction(ex);
            }
        }
    }
}