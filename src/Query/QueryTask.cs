using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;

namespace QueryHelper
{
    public class QueryTask
    {
        public QueryTask(SQLQuery query, Task<int> nonQueryTask)
        {
            this.Query = query;
            this.NonQueryTask = nonQueryTask;
            this.ReaderTask = null;
            this.ScalerTask = null;
        }
        public QueryTask(SQLQuery query, Task<DbDataReader> readerTask)
        {
            this.Query = query;
            this.NonQueryTask = null;
            this.ReaderTask = readerTask;
            this.ScalerTask = null;
        }
        public QueryTask(SQLQuery query, Task<object> scalerTask)
        {
            this.Query = query;
            this.NonQueryTask = null;
            this.ReaderTask = null;
            this.ScalerTask = scalerTask;
        }
        //TODO: this needs refactoring... maybe a base class.
        public readonly SQLQuery Query;
        public readonly Task<int> NonQueryTask;
        public readonly Task<DbDataReader> ReaderTask;
        public readonly Task<object> ScalerTask;
        public Task Task
        {
            get
            {
                if (NonQueryTask != null) return NonQueryTask;
                if (ReaderTask != null) return ReaderTask;
                return ScalerTask;
            }
        }
    }
}
