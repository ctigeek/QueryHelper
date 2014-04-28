using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryHelper
{
    public class SQLQueryScaler<T> : SQLQuery, IScalerQuery
    {
        public SQLQueryScaler(string sql)
            : base(sql, SQLQueryType.Scaler)
        {
        }

        public T ReturnValue { get; set; }

        public void ProcessScalerResult(object result)
        {
            T returnResult = default(T);
            if (result != DBNull.Value)
            {
                if (result is T)
                {
                    returnResult = (T)result;
                }
                else if (typeof(T) == typeof(string))
                {
                    object stringResult = (object)result.ToString();
                    returnResult = (T)stringResult;
                }
            }
            this.ReturnValue = returnResult;
        }
    }
}
