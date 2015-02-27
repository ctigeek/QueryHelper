using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;

namespace QueryHelper
{
    public class SQLQuery
    {
        public SQLQuery(string sql, SQLQueryType queryType = SQLQueryType.DataReader)
        {
            this.OriginalSQL = sql;
            this.ModifiedSQL = OriginalSQL;
            SQLQueryType = queryType;
            Parameters = new Dictionary<string, object>();
            InParameters = new Dictionary<string, List<object>>();
            GroupNumber = 0;
            OrderNumber = 0;
            CausedAbort = false;
            Executed = false;
            CommandType = CommandType.Text;
        }
        public readonly Dictionary<string, object> Parameters;
        public readonly Dictionary<string, List<object>> InParameters;
        public readonly string OriginalSQL;
        public CommandType CommandType { get; set; }
        public string ModifiedSQL { get; set; }
        public readonly SQLQueryType SQLQueryType;
        public int RowCount { get; set; }
        public int GroupNumber { get; set; }
        public int OrderNumber { get; set; }
        public bool Executed { get; set; }
        public bool CausedAbort { get; set; }
        public object Tag { get; set; }
        private Action<SQLQuery> preQueryProcess;
        public virtual Action<SQLQuery> PreQueryProcess
        {
            get
            {
                if (preQueryProcess == null)
                {
                    preQueryProcess = new Action<SQLQuery>(query => { });
                }
                return preQueryProcess;
            }
            set
            {
                preQueryProcess = value;
            }
        }
        private Func<DbDataReader, bool> processRow;
        public virtual Func<DbDataReader, bool> ProcessRow
        {
            get
            {
                if (processRow == null)
                {
                    processRow = new Func<DbDataReader, bool>(dr => { return true; });
                }
                return processRow;
            }
            set
            {
                processRow = value;
            }
        }
        public virtual Func<DbDataReader, Task<bool>> ProcessRowAsync { get; set; }
        private Func<SQLQuery, bool> postQueryProcess;
        public virtual Func<SQLQuery, bool> PostQueryProcess
        {
            get
            {
                if (postQueryProcess == null)
                {
                    postQueryProcess = new Func<SQLQuery, bool>(query => { return true; });
                }
                return postQueryProcess;
            }
            set
            {
                postQueryProcess = value;
            }
        }
    }
}
