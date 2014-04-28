using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;
using Moq;
using Moq.Protected;

namespace test
{
    public class MockDatabaseFactory : DbProviderFactory
    {
        public static Mock<DbTransaction> DbTransaction { get; set; }
        public static Mock<MoqDbConnection> DbConnection { get; set; }
        public static Mock<DbCommand> DbCommand { get; set; }
        public static Mock<DbParameter> DbParameter { get; set; }
        public static Mock<DbParameterCollection> Parameters { get; set; }

        public override DbCommand CreateCommand()
        {
            return (DbCommand)DbCommand.Object;
        }
        public override DbConnection CreateConnection()
        {
            return DbConnection.Object;
        }
        public override DbParameter CreateParameter()
        {
            return DbParameter.Object;
        }

        public static Mock<MoqDataReader> CreateDbDataReader(TestDataContainer dataContainer)
        {
            var dataReader = new Mock<MoqDataReader>(dataContainer.dataRow);
            dataReader.CallBase = true;
            dataReader.Setup(dr => dr.Close());
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand(dataReader.Object);
            return dataReader;
        }
        public static Mock<DbTransaction> CreateDbTransaction()
        {
            var dbTransaction = new Mock<DbTransaction>();
            dbTransaction.CallBase = true;
            dbTransaction.Setup(dbt => dbt.Commit());
            dbTransaction.Setup(dbt => dbt.Rollback());
            return dbTransaction;
        }
        public static Mock<MoqDbConnection> CreateDbConnection()
        {
            var dbConnection = new Mock<MoqDbConnection>();
            dbConnection.CallBase = true;
            dbConnection.SetupProperty(dbc => dbc.ConnectionString);
            dbConnection.Setup(dbc => dbc.Open());
            dbConnection.Setup(dbc => dbc.Close());

            return dbConnection;
        }
        public static Mock<DbParameter> CreateDbParameter()
        {
            var dbParameter = new Mock<DbParameter>();
            dbParameter.SetupProperty(dbc => dbc.ParameterName);
            dbParameter.SetupProperty(dbc => dbc.Value);
            return dbParameter;
        }
        public static Mock<DbParameterCollection> CreateParameterCollection()
        {
            var parameters = new Mock<DbParameterCollection>();
            parameters.Setup(p => p.Add(It.IsAny<DbParameter>()));
            return parameters;
        }
        public static void SetScalerReturnValue(object value)
        {
            if (MockDatabaseFactory.DbCommand != null)
            {
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(value);
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()))
                   .ReturnsAsync(value);
            }
        }
        public static Mock<DbCommand> CreateDbCommand(DbDataReader dataReader = null)
        {
            var dbCommand = new Mock<DbCommand>();
            //http://blogs.clariusconsulting.net/kzu/mocking-protected-members-with-moq/
            if (dataReader != null)
            {
                dbCommand.Protected()
                    .Setup<DbDataReader>("ExecuteDbDataReader", It.IsAny<CommandBehavior>())
                    .Returns(dataReader);
                dbCommand.Protected()
                    .Setup<Task<DbDataReader>>("ExecuteDbDataReaderAsync", It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>())
                    .Returns(Task.FromResult<DbDataReader>(dataReader));
            }
            dbCommand.Setup(dbc => dbc.ExecuteNonQuery())
                        .Returns(543);
            dbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                        .ReturnsAsync(345);

            dbCommand.Protected()
                .SetupGet<DbParameterCollection>("DbParameterCollection")
                .Returns(Parameters.Object);

            dbCommand.CallBase = true;
            dbCommand.SetupProperty(dbc => dbc.CommandText);
            return dbCommand;
        }
    }
}
