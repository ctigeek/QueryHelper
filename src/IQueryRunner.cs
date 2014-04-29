using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryHelper
{
    public interface IQueryRunner : ITransactable, IDisposable
    {
        Action<string, System.Diagnostics.TraceEventType> LogMessage { get; set; }
        bool DebugLoggingEnabled { get; set; }

        void RunQuery(SQLQuery query);
        void RunQuery(IEnumerable<SQLQuery> queries, bool withTransaction = false);
        Task RunQueryAsync(SQLQuery query);
        Task RunQueryAsync(IEnumerable<SQLQuery> queries, bool withTransaction = false);
        void RunNonQuery(string sql);
        T RunScalerQuery<T>(string sql);
        Task<T> RunScalerQueryAsync<T>(string sql);
        T RunScalerQuery<T>(SQLQueryScaler<T> query);
        Task<T> RunScalerQueryAsync<T>(SQLQueryScaler<T> query);
    }
}
