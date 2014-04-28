using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using NUnit.Framework;
using Moq.Protected;
using Moq;
using QueryHelper;

namespace test
{
    [TestFixture]
    public class Test
    {
        QueryRunner queryRunner;
        string connectionString = "connString";
        string provider = "SqlServerCe";
        string sqlString = "select someColumn from someTable;";
        string sqlInString = "select someColumn from someTable where someColumn in (@inParam);";
        string sqlInStringAfterProcessing = "select someColumn from someTable where someColumn in (@inParam0,@inParam1,@inParam2,@inParam3);";
        string logMessage;
        System.Diagnostics.TraceEventType logLevel;
        string scalerStringValue = "This is the return value.";

        [SetUp]
        public void Setup()
        {
            MockDatabaseFactory.DbParameter = MockDatabaseFactory.CreateDbParameter();
            MockDatabaseFactory.Parameters = MockDatabaseFactory.CreateParameterCollection();
            MockDatabaseFactory.DbConnection = MockDatabaseFactory.CreateDbConnection();
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbTransaction = MockDatabaseFactory.CreateDbTransaction();
            MockDatabaseFactory.SetScalerReturnValue(scalerStringValue);

            queryRunner = new QueryRunner(connectionString, provider, new MockDatabaseFactory());
            queryRunner.LogMessage = new Action<string, System.Diagnostics.TraceEventType>((message, level) =>
            {
                logMessage = message;
                logLevel = level;
            });
            queryRunner.DebugLoggingEnabled = true;
            logMessage = string.Empty;
            logLevel = System.Diagnostics.TraceEventType.Start;
        }

        [Test]
        public void ReadScalerReturnsAString()
        {
            var query = new SQLQueryScaler<string>(sqlString);
            var returnValue = queryRunner.RunScalerQuery<string>(query);

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringAsync()
        {
            var query = new SQLQueryScaler<string>(sqlString);
            var task = queryRunner.RunScalerQueryAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringUsingSQLString()
        {
            var returnValue = queryRunner.RunScalerQuery<string>(sqlString);

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringUsingSQLStringAsync()
        {
            var task = queryRunner.RunScalerQueryAsync<string>(sqlString);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringWithParameters()
        {
            var query = new SQLQueryScaler<string>(sqlString);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var returnValue = queryRunner.RunScalerQuery<string>(query);

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringWithParametersAsync()
        {
            var query = new SQLQueryScaler<string>(sqlString);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var task = queryRunner.RunScalerQueryAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsString()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryScaler<string>(sqlString);
            var returnValue = queryRunner.RunScalerQuery<string>(query);

            Assert.AreEqual(scalerReturn.ToString(), returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsStringAsync()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryScaler<string>(sqlString);

            var task = queryRunner.RunScalerQueryAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerReturn.ToString(), returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsInt()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryScaler<int>(sqlString);
            var returnValue = queryRunner.RunScalerQuery<int>(query);

            Assert.AreEqual(scalerReturn, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsIntAsync()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryScaler<int>(sqlString);
            var task = queryRunner.RunScalerQueryAsync<int>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerReturn, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerDbNullReturnsNullObject()
        {
            DBNull scalerReturn = DBNull.Value;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryScaler<string>(sqlString);
            var returnValue = queryRunner.RunScalerQuery<string>(query);

            Assert.IsNull(returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerDbNullReturnsNullObjectAsync()
        {
            DBNull scalerReturn = DBNull.Value;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryScaler<string>(sqlString);
            var task = queryRunner.RunScalerQueryAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.IsNull(returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadSingleRowNoParameters()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQuery(sqlString, SQLQueryType.DataReader)
            {
                ProcessRow = dataContainer.ProcessRow
            };
            queryRunner.RunQuery(query);

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Protected()
                .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowNoParametersAsync()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQuery(sqlString, SQLQueryType.DataReader)
            {
                ProcessRow = dataContainer.ProcessRow
            };
            var task = queryRunner.RunQueryAsync(query);
            task.Wait();

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Protected()
                .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithInParameters()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQuery(sqlInString, SQLQueryType.DataReader)
            {
                ProcessRow = dataContainer.ProcessRow
            };
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var inList = new List<object>();
            inList.AddRange(new string[] { "val1", "val2", "val3", "val4" });
            query.InParameters.Add("inParam", inList);
            queryRunner.RunQuery(query);

            dataContainer.AssertData();
            VerifyLogging(sqlInStringAfterProcessing);

            Assert.AreEqual(sqlInStringAfterProcessing, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(7));
            MockDatabaseFactory.DbCommand.Protected()
                             .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithInParametersAsync()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQuery(sqlInString, SQLQueryType.DataReader)
            {
                ProcessRow = dataContainer.ProcessRow
            };
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var inList = new List<object>();
            inList.AddRange(new string[] { "val1", "val2", "val3", "val4" });
            query.InParameters.Add("inParam", inList);
            var task = queryRunner.RunQueryAsync(query);
            task.Wait();

            dataContainer.AssertData();
            VerifyLogging(sqlInStringAfterProcessing);

            Assert.AreEqual(sqlInStringAfterProcessing, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(7));
            MockDatabaseFactory.DbCommand.Protected()
                            .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithParameters()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQuery(sqlString, SQLQueryType.DataReader)
            {
                ProcessRow = dataContainer.ProcessRow
            };
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            queryRunner.RunQuery(query);

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Protected()
                            .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithParametersAsync()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQuery(sqlString, SQLQueryType.DataReader)
            {
                ProcessRow = dataContainer.ProcessRow
            };
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var task = queryRunner.RunQueryAsync(query);
            task.Wait();

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Protected()
                            .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            MockDatabaseFactory.DbCommand.Protected()
                .VerifyGet<DbParameterCollection>("DbParameterCollection", Times.Exactly(3));
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void NonQueryTestNoParameters()
        {
            var query = new SQLQuery(sqlString, SQLQueryType.NonQuery);

            queryRunner.RunQuery(query);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestNoParametersAsync()
        {
            var query = new SQLQuery(sqlString, SQLQueryType.NonQuery);

            var task = queryRunner.RunQueryAsync(query);
            task.Wait();

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParameters()
        {
            var query = new SQLQuery(sqlString, SQLQueryType.NonQuery);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            queryRunner.RunQuery(query);

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersAsync()
        {
            var query = new SQLQuery(sqlString, SQLQueryType.NonQuery);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var task = queryRunner.RunQueryAsync(query);
            task.Wait();

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersBuiltDynamically()
        {
            var query = new SQLQuery(sqlString, SQLQueryType.NonQuery);
            query.PreQueryProcess = new Action<SQLQuery>(q =>
            {
                q.Parameters.Add("param1", "value1");
                q.Parameters.Add("param2", "value2");
                q.Parameters.Add("param3", 333);
            });

            queryRunner.RunQuery(query);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersBuiltDynamicallyAsync()
        {
            var query = new SQLQuery(sqlString, SQLQueryType.NonQuery);
            query.PreQueryProcess = new Action<SQLQuery>(q =>
            {
                q.Parameters.Add("param1", "value1");
                q.Parameters.Add("param2", "value2");
                q.Parameters.Add("param3", 333);
            });

            var task = queryRunner.RunQueryAsync(query);
            task.Wait();

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTransactionNoParameters()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() =>
                {
                    returnValue++;
                    return Task.FromResult<int>(returnValue);
                });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(counter, new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter });
            }
            queryRunner.RunQuery(queries.Values, true);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionNoParametersAsync()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                        .Returns(() =>   //you can't use returnasync here becuse there's no way to increment the variable each time, not even with .callback.
                        {
                            returnValue++;
                            return Task.FromResult<int>(returnValue);
                        });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(counter, new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter });
            }
            var task = queryRunner.RunQueryAsync(queries.Values, true);
            task.Wait();

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionWithParameters()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                        .Returns(() =>   //you can't use returnasync here becuse there's no way to increment the variable each time, not even with .callback.
                        {
                            returnValue++;
                            return Task.FromResult<int>(returnValue);
                        });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                var query = new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter };
                query.Parameters.Add("param1", "value1");
                query.Parameters.Add("param2", "value2");
                query.Parameters.Add("param3", 333);
                queries.Add(counter, query);
            }
            queryRunner.RunQuery(queries.Values, true);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(30));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionWithParametersAsync()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                       .Returns(() =>
                       {
                           returnValue++;
                           return Task.FromResult<int>(returnValue);
                       });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                var query = new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter };
                query.Parameters.Add("param1", "value1");
                query.Parameters.Add("param2", "value2");
                query.Parameters.Add("param3", 333);
                queries.Add(counter, query);
            }
            var task = queryRunner.RunQueryAsync(queries.Values, true);
            task.Wait();

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(30));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test, ExpectedException("System.ApplicationException", UserMessage = "blah blah")]
        public void NonQueryTransactionNoParametersRollbackWhenException()
        {
            try
            {
                int returnValue = 100;
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                      .Returns(() =>
                      {
                          returnValue++;
                          if (returnValue == 105) throw new ApplicationException("blah blah");
                          return Task.FromResult<int>(returnValue);
                      });

                var queries = new Dictionary<int, SQLQuery>();
                for (int counter = 0; counter < 10; counter++)
                {
                    var query = new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter };
                    queries.Add(counter, query);
                }
                queryRunner.RunQuery(queries.Values, true);
            }
            catch (System.AggregateException ex)
            {
                throw ex.InnerExceptions[0];
            }
            finally
            {
                MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
                MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(5));
                MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
                MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(1));
            }
        }

        [Test, ExpectedException("System.ApplicationException", UserMessage = "blah blah")]
        public void NonQueryTransactionNoParametersRollbackWhenExceptionAsync()
        {
            try
            {
                int returnValue = 100;
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                       .Returns(() =>
                       {
                           returnValue++;
                           if (returnValue == 105) throw new ApplicationException("blah blah");
                           return Task.FromResult<int>(returnValue);
                       });

                var queries = new Dictionary<int, SQLQuery>();
                for (int counter = 0; counter < 10; counter++)
                {
                    var query = new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter };
                    queries.Add(counter, query);
                }
                var task = queryRunner.RunQueryAsync(queries.Values, true);
                task.Wait();
            }
            catch (System.AggregateException ex)
            {
                throw ex.InnerExceptions[0];
            }
            finally
            {
                MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
                MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(5));
                MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
                MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(1));
            }
        }

        [Test]
        public void NonQueryExternalTransactionAsync()
        {
            var transaction = new CommittableTransaction();
            queryRunner.EnlistTransaction(transaction);

            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() =>
                {
                    returnValue++;
                    return Task.FromResult<int>(returnValue);
                });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(counter, new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter });
            }
            queryRunner.RunQuery(queries.Values);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(0));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            Assert.IsTrue(queryRunner.TransactionOpen);

            transaction.Commit();
            Assert.AreEqual(1, MockDatabaseFactory.DbConnection.Object.CommitCallCount);
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            Assert.IsFalse(queryRunner.TransactionOpen);
        }

        [Test]
        public void NonQueryExternalTransactionRollbackAsync()
        {
            var transaction = new CommittableTransaction();
            queryRunner.EnlistTransaction(transaction);

            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() =>
                {
                    returnValue++;
                    return Task.FromResult<int>(returnValue);
                });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(counter, new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter });
            }
            queryRunner.RunQuery(queries.Values);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(0));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            Assert.IsTrue(queryRunner.TransactionOpen);

            transaction.Rollback();

            Assert.AreEqual(1, MockDatabaseFactory.DbConnection.Object.RollbackCallCount);
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            Assert.IsFalse(queryRunner.TransactionOpen);
        }

        [Test]
        public void NonQueryExternalTransactionMultipleRunsAsync()
        {
            var transaction = new CommittableTransaction();
            queryRunner.EnlistTransaction(transaction);

            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() =>
                {
                    returnValue++;
                    return Task.FromResult<int>(returnValue);
                });

            var queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(counter, new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter });
            }
            queryRunner.RunQuery(queries.Values);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            Assert.IsTrue(queryRunner.TransactionOpen);


            queries = new Dictionary<int, SQLQuery>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(counter, new SQLQuery("insert into sometable values (" + counter + ");", SQLQueryType.NonQuery) { GroupNumber = counter });
            }
            queryRunner.RunQuery(queries.Values);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(111 + counter, queries[counter].RowCount);
            }
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(20));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            Assert.IsTrue(queryRunner.TransactionOpen);

            transaction.Commit();
            Assert.AreEqual(1, MockDatabaseFactory.DbConnection.Object.CommitCallCount);
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            Assert.IsFalse(queryRunner.TransactionOpen);
        }

        private void VerifyLogging(string sql)
        {
            Assert.True(this.logMessage.Contains(sql));
            Assert.AreEqual(System.Diagnostics.TraceEventType.Verbose, this.logLevel);
        }
    }
}
