using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;
using System.Threading.Tasks;
using System.Data.Common;
using System.Configuration;

namespace QueryHelper
{
    public class QueryRunner : IQueryRunner
    {
        public readonly string ConnectionString;
        public readonly string DataProvider;
        public readonly DbProviderFactory DbFactory;
        public bool DebugLoggingEnabled { get; set; }
        public Action<string, System.Diagnostics.TraceEventType> LogMessage { get; set; }

        public QueryRunner(string connectionString, string dataProvider, DbProviderFactory dbFactory)
        {
            if (dbFactory == null) throw new ArgumentException("dbFactory not found.", "dbFactory");
            this.ConnectionString = connectionString;
            this.DataProvider = dataProvider;
            this.DbFactory = dbFactory;
            DebugLoggingEnabled = false;
            TransactionOpen = false;
        }

        public QueryRunner(string connectionString, string dataProvider) :
            this(connectionString, dataProvider, DbProviderFactories.GetFactory(dataProvider))
        {
        }

        public QueryRunner(string connectionName)
        {
            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
            {
                throw new ArgumentException("'" + connectionName + "' is not a valid connection string name. Check your configuration file to make sure it exists.", "connectionName");
            }
            this.ConnectionString = ConfigurationManager.ConnectionStrings[connectionName].ConnectionString;
            this.DataProvider = ConfigurationManager.ConnectionStrings[connectionName].ProviderName;
            this.DbFactory = DbProviderFactories.GetFactory(DataProvider);
            DebugLoggingEnabled = false;
            TransactionOpen = false;
        }

        public void Dispose()
        {
            if (TransactionOpen)
            {
                CloseConnection(persistentConnection);
            }
        }

        #region external transactions
        private DbConnection persistentConnection;
        private Transaction currentTransaction;
        public bool TransactionOpen { get; private set; }
        public async void EnlistTransaction(Transaction transaction)
        {
            if (TransactionOpen)
            {
                throw new InvalidOperationException("There is already a transaction in progress.");
            }
            TransactionOpen = true;
            var conn = await GetOpenConnectionAsync();
            conn.EnlistTransaction(transaction);
            this.currentTransaction = transaction;
            this.currentTransaction.TransactionCompleted += currentTransaction_TransactionCompleted;
        }
        void currentTransaction_TransactionCompleted(object sender, TransactionEventArgs e)
        {
            TransactionOpen = false;
            CloseConnection(persistentConnection);
            persistentConnection = null;
            currentTransaction = null;
        }
        #endregion
        public void RunNonQuery(string sql)
        {
            RunQuery(new[] { new SQLQuery(sql, SQLQueryType.NonQuery) }, TransactionOpen);
        }
        public void RunQuery(SQLQuery query)
        {
            RunQuery(new[] { query }, TransactionOpen);
        }
        
        public async Task RunQueryAsync(SQLQuery query)
        {
            await RunQueryAsync(new[] { query }, TransactionOpen);
        }

        public void RunQuery(IEnumerable<SQLQuery> queries, bool withTransaction = false)
        {
            DbTransaction transaction = null;
            DbConnection connection = null;
            try
            {
                ValidateQueriesForSync(queries);
                connection = GetOpenConnection();
                if (withTransaction && !TransactionOpen)
                {
                    transaction = GetTransaction(connection);
                }
                foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn).ToArray())
                {
                    var taskList = BuildQueryTaskList(queries, groupNum, connection, transaction);
                    Task.WaitAll(taskList.Select(qt => qt.Task).ToArray());
                    foreach (var queryTask in taskList)
                    {
                        if (queryTask.Query.SQLQueryType == SQLQueryType.DataReader)
                        {
                            ProcessReadQuery(queryTask);
                        }
                        else
                        {
                            ProcessQuery(queryTask);
                        }
                        queryTask.Query.CausedAbort = !queryTask.Query.PostQueryProcess(queryTask.Query);
                    }
                    if (taskList.Exists(qt => qt.Query.CausedAbort == true))
                    {
                        break;
                    }
                }
                CommitDbTransaction(transaction);
            }
            catch (Exception exception)
            {
                try
                {
                    RollbackDbTransaction(transaction);
                }
                catch (Exception rollbackException)
                {
                    throw new AggregateException(exception, rollbackException);
                }
                throw;
            }
            finally
            {
                CloseConnection(connection);
            }
        }

        public async Task RunQueryAsync(IEnumerable<SQLQuery> queries, bool withTransaction = false)
        {
            DbTransaction transaction = null;
            DbConnection connection = null;
            try
            {
                connection = await GetOpenConnectionAsync();
                if (withTransaction && !TransactionOpen)
                {
                    transaction = GetTransaction(connection);
                }
                foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn).ToArray())
                {
                    var taskList = BuildQueryTaskList(queries, groupNum, connection, transaction);
                    await Task.WhenAll(taskList.Select(qt => qt.Task));
                    foreach (var queryTask in taskList)
                    {
                        if (queryTask.Query.SQLQueryType == SQLQueryType.DataReader)
                        {
                            await ProcessReadQueryAsync(queryTask);
                        }
                        else
                        {
                            ProcessQuery(queryTask);
                        }
                        queryTask.Query.CausedAbort = !queryTask.Query.PostQueryProcess(queryTask.Query);
                    }
                    if (taskList.Exists(qt => qt.Query.CausedAbort == true))
                    {
                        break;
                    }
                }
                CommitDbTransaction(transaction);
            }
            catch (Exception exception)
            {
                try
                {
                    RollbackDbTransaction(transaction);
                }
                catch (Exception rollbackException)
                {
                    throw new AggregateException(exception, rollbackException);
                }
                throw;
            }
            finally
            {
                CloseConnection(connection);
            }
        }

        private List<QueryTask> BuildQueryTaskList(IEnumerable<SQLQuery> queries, int groupNum, DbConnection connection, DbTransaction transaction)
        {
            return queries
                .Where(q => q.GroupNumber == groupNum)
                .OrderBy(q => q.OrderNumber)
                .Select(query => ExecuteQuery(query, connection, transaction))
                .ToList();
        }

        private void RollbackDbTransaction(DbTransaction transaction)
        {
            if (!TransactionOpen && transaction != null)
            {
                transaction.Rollback();
            }
        }

        private void CommitDbTransaction(DbTransaction transaction)
        {
            if (!TransactionOpen && transaction != null)
            {
                transaction.Commit();
            }
        }

        private void ValidateQueriesForSync(IEnumerable<SQLQuery> queries)
        {
            if (queries.Any(q => q.ProcessRowAsync != null))
            {
                throw new ArgumentException("SQLQuery.ProcessRowAsync must be null when calling RunQuery. It can only be non-null when calling RunQueryAsync.");
            }
        }

        private DbTransaction GetTransaction(DbConnection connection)
        {
            var transaction = connection.BeginTransaction();
            return transaction;
        }

        private void CloseConnection(DbConnection connection)
        {
            if (!TransactionOpen)
            {
                persistentConnection = null;
                if (connection != null)
                {
                    connection.Close();
                    connection.Dispose();
                }
            }
        }

        private DbConnection GetOpenConnection()
        {
            if (TransactionOpen && persistentConnection != null)
            {
                return persistentConnection;
            }
            else
            {
                var conn = CreateConnection();
                conn.Open();
                if (TransactionOpen)
                {
                    persistentConnection = conn;
                }
                return conn;
            }
        }

        private async Task<DbConnection> GetOpenConnectionAsync()
        {
            if (TransactionOpen && persistentConnection != null)
            {
                return await Task.FromResult<DbConnection>(persistentConnection);
            }
            else
            {
                var conn = CreateConnection();
                await conn.OpenAsync();
                if (TransactionOpen)
                {
                    persistentConnection = conn;
                }
                return conn;
            }
        }

        private void ProcessReadQuery(QueryTask queryTask)
        {
            if (queryTask.Query.SQLQueryType == SQLQueryType.DataReader)
            {
                queryTask.Query.RowCount = 0;
                using (var reader = queryTask.ReaderTask.Result)
                {
                    while (reader.Read())
                    {
                        queryTask.Query.RowCount++;
                        if (!queryTask.Query.ProcessRow(reader))
                        {
                            break;
                        }
                    }
                    reader.Close();
                }
            }
        }

        private async Task ProcessReadQueryAsync(QueryTask queryTask)
        {
            if (queryTask.Query.SQLQueryType == SQLQueryType.DataReader)
            {
                queryTask.Query.RowCount = 0;
                using (var reader = queryTask.ReaderTask.Result)
                {
                    while (await reader.ReadAsync())
                    {
                        queryTask.Query.RowCount++;
                        if (!queryTask.Query.ProcessRow(reader))
                        {
                            break;
                        }
                        if (queryTask.Query.ProcessRowAsync != null && !await queryTask.Query.ProcessRowAsync(reader))
                        {
                            break;
                        }
                    }
                    reader.Close();
                }
            }
        }

        private void ProcessQuery(QueryTask queryTask)
        {
            if (queryTask.Query.SQLQueryType == SQLQueryType.NonQuery)
            {
                queryTask.Query.RowCount = queryTask.NonQueryTask.Result;
            }
            else if (queryTask.Query.SQLQueryType == SQLQueryType.Scaler)
            {
                queryTask.Query.RowCount = 1;
                var result = queryTask.ScalerTask.Result;
                if (queryTask.Query is IScalerQuery)
                {
                    IScalerQuery scalerQuery = (IScalerQuery)queryTask.Query;
                    scalerQuery.ProcessScalerResult(result);
                }
            }
        }

        private QueryTask ExecuteQuery(SQLQuery query, DbConnection connection, DbTransaction transaction)
        {
            query.PreQueryProcess(query);
            var command = CreateCommand(query, connection, transaction);
            DumpSqlAndParamsToLog(query);
            if (query.SQLQueryType == SQLQueryType.NonQuery)
            {
                var task = command.ExecuteNonQueryAsync();
                query.Executed = true;
                return new QueryTask(query, task);
            }
            else if (query.SQLQueryType == SQLQueryType.DataReader)
            {
                var task = command.ExecuteReaderAsync();
                query.Executed = true;
                return new QueryTask(query, task);
            }
            else
            {
                var task = command.ExecuteScalarAsync();
                query.Executed = true;
                return new QueryTask(query, task);
            }
        }

        public T RunScalerQuery<T>(string sql)
        {
            return RunScalerQuery<T>(new SQLQueryScaler<T>(sql));
        }

        public T RunScalerQuery<T>(SQLQueryScaler<T> query)
        {
            var task = RunScalerQueryAsync<T>(query);
            task.Wait();
            return task.Result;
        }

        public async Task<T> RunScalerQueryAsync<T>(string sql)
        {
            var query = new SQLQueryScaler<T>(sql);
            await RunQueryAsync(query);
            return query.ReturnValue;
        }

        public async Task<T> RunScalerQueryAsync<T>(SQLQueryScaler<T> query)
        {
            await RunQueryAsync(query);
            return query.ReturnValue;
        }

        private List<SQLQuery> SetQueryGroupsForSyncOperation(IEnumerable<SQLQuery> queries)
        {
            int groupNum = 1;
            var queryList = queries.ToList();
            foreach (var query in queryList.OrderBy(q => q.GroupNumber).ThenBy(q => q.OrderNumber))
            {
                query.GroupNumber = groupNum;
                query.OrderNumber = 1;
                groupNum++;
            }
            return queryList;
        }

        private void AddParameters(DbCommand command, SQLQuery query)
        {
            foreach (string paramName in query.InParameters.Keys)
            {
                if (query.InParameters[paramName] != null && query.InParameters[paramName].Any() && query.ModifiedSQL.Contains("@" + paramName))
                {
                    var parameterDictionary = new Dictionary<string, object>();
                    foreach (var value in query.InParameters[paramName])
                    {
                        parameterDictionary.Add(string.Format("{0}{1}", paramName, parameterDictionary.Count), value);
                    }
                    //TODO: this needs to be a regex otherwise it could be prone to replacing paramName that starts with the same name eg. @myvar & @myvar2.
                    query.ModifiedSQL = query.ModifiedSQL.Replace("@" + paramName, string.Join(",", parameterDictionary.Select(pd => "@" + pd.Key)));
                    foreach (var parameter in parameterDictionary.Keys)
                    {
                        query.Parameters.Add(parameter, parameterDictionary[parameter]);
                    }
                }
            }
            foreach (string paramName in query.Parameters.Keys)
            {
                command.Parameters.Add(CreateParameter(paramName, query.Parameters[paramName]));
            }
        }

        private DbParameter CreateParameter(string name, object value)
        {
            var parameter = DbFactory.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }

        private DbConnection CreateConnection()
        {
            var connection = DbFactory.CreateConnection();
            connection.ConnectionString = ConnectionString;
            return connection;
        }

        private DbCommand CreateCommand(SQLQuery query, DbConnection connection, DbTransaction transaction = null)
        {
            var command = DbFactory.CreateCommand();
            command.Connection = connection;
            AddParameters(command, query);
            command.CommandText = query.ModifiedSQL;
            command.CommandType = query.CommandType;
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            return command;
        }

        private void DumpSqlAndParamsToLog(SQLQuery query)
        {
            if (LogMessage == null || !DebugLoggingEnabled) return;

            var sb = new System.Text.StringBuilder();
            sb.AppendFormat("About to execute \"{0}\" ", query.ModifiedSQL);
            if (query.Parameters.Count > 0)
            {
                sb.Append(" with parameters:\r\n");
                foreach (string key in query.Parameters.Keys)
                {
                    sb.AppendFormat("{0}={1} with type {2}.\r\n", key, query.Parameters[key], query.Parameters[key].GetType());
                }
            }
            else
            {
                sb.Append(" with no parameters.");
            }
            LogDebug(sb.ToString());
        }

        private void LogWarn(string message)
        {
            if (LogMessage != null)
            {
                LogMessage(message, System.Diagnostics.TraceEventType.Warning);
            }
        }
        private void LogError(string message)
        {
            if (LogMessage != null)
            {
                LogMessage(message, System.Diagnostics.TraceEventType.Error);
            }
        }
        private void LogDebug(string message)
        {
            if (LogMessage != null && DebugLoggingEnabled)
            {
                LogMessage(message, System.Diagnostics.TraceEventType.Verbose);
            }
        }
    }
}
