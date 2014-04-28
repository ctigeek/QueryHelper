using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QueryHelper;
using Moq;
using NUnit.Framework;
using Example.Model;

namespace Example.Test
{
    [TestFixture]
    public class StoreRepositoryTests
    {
        StoreRepository repo;
        Mock<IQueryRunner> queryRunner;
        Customer customer;
        Order order;
        OrderItem orderItem1;
        OrderItem orderItem2;

        [SetUp]
        public void Setup()
        {
            customer = new Customer() { PK = 123 };
            orderItem1 = new OrderItem() { PK = 0, InventoryID = 123, Price = 232, Quantity = 999 };
            orderItem2 = new OrderItem() { PK = 0, InventoryID = 124, Price = 233, Quantity = 988 };
            order = new Order() { PK = 0, Customer = customer, OrderDatetime = DateTime.Now, Status = OrderStatus.Ordered };
            order.OrderItems.Add(orderItem1);
            orderItem1.Order = order;
            orderItem2.Order = order;
            order.OrderItems.Add(orderItem2);

            queryRunner = new Mock<IQueryRunner>();
            queryRunner.Setup(qh => qh.RunScalerQuery<int>(It.IsAny<SQLQueryScaler<int>>()))
                .Returns(123);

            repo = new StoreRepository(queryRunner.Object);
        }

        [Test]
        public void GetNumberOfOrdersWithStatusTest()
        {
            var val = repo.GetNumberOfOrdersWithStatus(OrderStatus.Ordered);
            Assert.AreEqual(123, val);
            queryRunner.Verify(qh =>
                qh.RunScalerQuery<int>(It.Is<SQLQueryScaler<int>>(q => q.ModifiedSQL == StoreRepository.SQLCountOrderOfStatus)));
            queryRunner.Verify(qh =>
                    qh.RunScalerQuery<int>(It.Is<SQLQueryScaler<int>>(s =>
                        s.Parameters.Count == 1 &&
                        (OrderStatus)s.Parameters["OrderStatus"] == OrderStatus.Ordered)),
                Times.Exactly(1));
        }

        [Test]
        public void GetOrderCountForAllStatusTest()
        {
            bool allTrue = true;
            queryRunner.Setup(qh => qh.RunQuery(It.IsAny<SQLQuery>()))
                .Callback<SQLQuery>(q =>
                {
                    allTrue = allTrue && q.ProcessRow(GetStatusCountDataReader(OrderStatus.BackOrdered, 111));
                    allTrue = allTrue && q.ProcessRow(GetStatusCountDataReader(OrderStatus.Destroyed, 222));
                    allTrue = allTrue && q.ProcessRow(GetStatusCountDataReader(OrderStatus.Forgotten, 333));
                    allTrue = allTrue && q.ProcessRow(GetStatusCountDataReader(OrderStatus.Lost, 444));
                    allTrue = allTrue && q.ProcessRow(GetStatusCountDataReader(OrderStatus.Ordered, 555));
                });
            var startDate = new DateTime(2000, 1, 1);
            var endDate = new DateTime(2000, 3, 3);
            var orderCountDict = repo.GetOrderCountForAllStatus(startDate, endDate);
            Assert.AreEqual(111, orderCountDict[OrderStatus.BackOrdered]);
            Assert.AreEqual(222, orderCountDict[OrderStatus.Destroyed]);
            Assert.AreEqual(333, orderCountDict[OrderStatus.Forgotten]);
            Assert.AreEqual(444, orderCountDict[OrderStatus.Lost]);
            Assert.AreEqual(555, orderCountDict[OrderStatus.Ordered]);
            Assert.IsTrue(allTrue);
            queryRunner.Verify(qh =>
                qh.RunQuery(It.Is<SQLQuery>(q => q.ModifiedSQL == StoreRepository.SQLCountOrderAllStatusByDate)));
            queryRunner.Verify(qh =>
                qh.RunQuery(It.Is<SQLQuery>(q => q.Parameters.Count == 2 &&
                                            (DateTime)q.Parameters["StartDate"] == startDate &&
                                            (DateTime)q.Parameters["EndDate"] == endDate)),
                            Times.Exactly(1));
        }

        [Test]
        public void AddNewOrderAndItemsTest()
        {
            List<SQLQuery> queriesParameter = null;
            //since the QueryHelper object is mocked, we have to do all the call-backs ourselves....
            queryRunner.Setup(qh => qh.RunQueryAsync(It.IsAny<List<SQLQuery>>(), false))
                .Returns(Task.FromResult<object>(null))
                .Callback<List<SQLQuery>, bool>((queries, useTrans) =>
                {
                    queriesParameter = queries; //save the queries to assert them afterwards....
                    foreach (var q in queries)
                    {
                        SQLQueryScaler<int> sqs = (SQLQueryScaler<int>)q;
                        //1. call the pre-query Func.
                        sqs.PreQueryProcess(sqs);
                        //2. set the scaler return value.
                        if (sqs.OrderNumber == 1)
                        {
                            sqs.ReturnValue = 12321;
                        }
                        else
                        {
                            sqs.ReturnValue = (sqs.OrderNumber == 2) ? 33 : 34;
                        }
                        //3. call the post-query Action.
                        q.PostQueryProcess(sqs);
                    }
                });

            var savedOrder = repo.AddNewOrderAndItems(order);
            queryRunner.Verify(qh => qh.RunQueryAsync(It.IsAny<List<SQLQuery>>(), false), Times.Exactly(1));
            Assert.AreEqual(12321, savedOrder.PK);
            Assert.AreEqual(33, savedOrder.OrderItems[0].PK);
            Assert.AreEqual(34, savedOrder.OrderItems[1].PK);
            Assert.AreEqual(3, queriesParameter.Count);
            Assert.AreEqual(1, queriesParameter.Count(q => q.GroupNumber == 1));
            Assert.AreEqual(2, queriesParameter.Count(q => q.GroupNumber == 2));
            AssertParametersAreSameAsObject(queriesParameter, false);
            Assert.AreEqual(StoreRepository.SQLInsertOrder, queriesParameter.FirstOrDefault(q => q.GroupNumber == 1).ModifiedSQL);
            Assert.AreEqual(StoreRepository.SQLInsertOrderItem, queriesParameter.FirstOrDefault(q => q.GroupNumber == 2).ModifiedSQL);
        }

        private void AssertParametersAreSameAsObject(IEnumerable<SQLQuery> queries, bool testPK)
        {
            foreach (var query in queries)
            {
                if (query.Tag != null)
                {
                    if (query.Tag is Order)
                    {
                        AssertParametersAreSameAsOrder((Order)query.Tag, query.Parameters, testPK);
                    }
                    else if (query.Tag is OrderItem)
                    {
                        AssertParametersAreSameAsOrderItem((OrderItem)query.Tag, query.Parameters, testPK);
                    }
                }
            }
        }
        private void AssertParametersAreSameAsOrderItem(OrderItem orderItem, Dictionary<string, object> parameters, bool testPK)
        {
            Assert.AreEqual(orderItem.InventoryID, (int)parameters["Inventory_PK"]);
            Assert.AreEqual(orderItem.Order.PK, (int)parameters["Order_PK"]);
            Assert.AreEqual(orderItem.Price, (long)parameters["PricePer"]);
            Assert.AreEqual(orderItem.Quantity, (int)parameters["Quantity"]);
            if (testPK)
            {
                Assert.AreEqual(orderItem.PK, (int)parameters["PK"]);
            }
        }
        private void AssertParametersAreSameAsOrder(Order order, Dictionary<string, object> parameters, bool testPK)
        {
            Assert.AreEqual(order.Customer.PK, (int)parameters["CustomerPK"]);
            Assert.AreEqual(order.OrderDatetime, (DateTime)parameters["Datetime"]);
            Assert.AreEqual((int)order.Status, (int)parameters["Status"]);
            if (testPK)
            {
                Assert.AreEqual(order.PK, (int)parameters["PK"]);
            }
        }

        private TestDataReader GetStatusCountDataReader(OrderStatus status, int count)
        {
            var returnValues = new Dictionary<string, object>();
            returnValues.Add("Status", (int)status);
            returnValues.Add("count", count);
            return new TestDataReader(returnValues);
        }
    }

    public class TestDataReader : System.Data.Common.DbDataReader
    {
        private Dictionary<string, object> ReturnValues;
        public TestDataReader(Dictionary<string, object> returnValues)
        {
            this.ReturnValues = returnValues;
        }

        public override object this[string name]
        {
            get { return ReturnValues[name]; }
        }

        //unused methods that have to be overridden.

        public override bool Read()
        {
            throw new NotImplementedException();
        }
        public override bool NextResult()
        {
            throw new NotImplementedException();
        }
        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }
        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override int GetInt32(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override void Close()
        {
            throw new NotImplementedException();
        }
        public override int Depth
        {
            get { throw new NotImplementedException(); }
        }
        public override int FieldCount
        {
            get { throw new NotImplementedException(); }
        }
        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool HasRows
        {
            get { throw new NotImplementedException(); }
        }

        public override bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public override int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }
    }
}
