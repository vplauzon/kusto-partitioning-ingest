﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoPartitionIngest
{
    public interface ICompleter
    {
        void Complete();

        void SetException(Exception ex);
    }
}