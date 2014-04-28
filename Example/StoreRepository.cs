using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QueryHelper;
using System.Data.Common;
using Example.Model;

namespace Example
{
    public class StoreRepository
    {
        public const string SQLCountOrderOfStatus = "select count(*) as count from Orders where Status = @OrderStatus;";
        public const string SQLCountOrderAllStatusByDate = "select count(*) as count, Status from Orders where OrderDateTime >= @Stardate and OrderDateTime <= @Enddate group by Status;";
        public const string SQLSelectSingleCustomer = "select * from Customers where PK=@CustomerId;";
        public const string SQLUpdateSingleCustomer = "update Customers set Name=@Name, EmailAddress=@EmailAddress, Password=@Password where PK=@CustomerId;";
        public const string SQLCreateCustomer = "insert into Customers (Name,EmailAddress,Password) OUTPUT Inserted.PK values (@Name, @EmailAddress, @Password);";
        public const string SQLInsertOrder = "insert into Orders (OrderDatetime,CustomerPK,Status) OUTPUT Inserted.PK values (@Datetime,@CustomerPK,@Status);";
        public const string SQLUpdateOrder = "update orders set OrderDateTime=@Datetime, Status=@Status, CustomerPK=@CustomerPK where PK = @PK;";
        public const string SQLInsertOrderItem = "insert into OrderItems (Order_PK,Quantity,Inventory_PK,PricePer) OUTPUT Inserted.PK values (@Order_PK,@Quantity,@Inventory_PK,@PricePer);";
        public const string SQLUpdateOrderItem = "update OrderItems set Order_PK=@Order_PK, Quantity=@Quantity, Inventory_PK=@Inventory_PK, PricePer=@PricePer where PK=@PK;";

        private IQueryRunner queryRunner;
        public StoreRepository(IQueryRunner qr)
        {
            this.queryRunner = qr;
        }

        public int GetNumberOfOrdersWithStatus(OrderStatus orderStatus)
        {
            var query = new SQLQueryScaler<int>(SQLCountOrderOfStatus);
            query.Parameters.Add("OrderStatus", (int)orderStatus);
            var count = queryRunner.RunScalerQuery<int>(query);
            return count;
        }
        public Dictionary<OrderStatus, int> GetOrderCountForAllStatus(DateTime startDate, DateTime endDate)
        {
            var returnDict = new Dictionary<OrderStatus, int>();
            var query = new SQLQuery(SQLCountOrderAllStatusByDate, SQLQueryType.DataReader);
            query.ProcessRow = new Func<DbDataReader, bool>(dr =>
            {
                returnDict.Add((OrderStatus)(int)dr["Status"], (int)dr["count"]);
                return true;
            });
            query.Parameters.Add("StartDate", startDate);
            query.Parameters.Add("EndDate", endDate);
            queryRunner.RunQuery(query);
            return returnDict;
        }
        public Order AddNewOrderAndItems(Order order)
        {
            int orderCounter = 1;
            var queries = new List<SQLQuery>();
            var query = GetOrderQuery(order);
            query.OrderNumber = orderCounter;
            query.GroupNumber = 1;
            queries.Add(query);
            foreach (OrderItem oi in order.OrderItems)
            {
                orderCounter++;
                var oiQuery = GetOrderItemQuery(oi);
                oiQuery.OrderNumber = orderCounter;
                oiQuery.GroupNumber = 2;
                queries.Add(oiQuery);
            }
            queryRunner.RunQueryAsync(queries)
                .Wait();

            return order;
        }

        private SQLQuery GetOrderItemQuery(OrderItem orderItem)
        {
            SQLQuery query = (orderItem.PK > 0) ?
                new SQLQuery(SQLUpdateOrderItem, SQLQueryType.NonQuery) :
                new SQLQueryScaler<int>(SQLInsertOrderItem);
            query.Parameters.Add("Quantity", orderItem.Quantity);
            query.Parameters.Add("Inventory_PK", orderItem.InventoryID);
            query.Parameters.Add("PricePer", orderItem.Price);
            if (orderItem.Order.PK <= 0)
            {
                query.PreQueryProcess = q =>
                {
                    query.Parameters.Add("Order_PK", orderItem.Order.PK);
                };
            }
            else
            {
                query.Parameters.Add("Order_PK", orderItem.Order.PK);
            }
            if (orderItem.PK > 0)
            {
                query.Parameters.Add("PK", orderItem.PK);
            }
            else
            {
                query.PostQueryProcess = q =>
                {
                    orderItem.PK = ((SQLQueryScaler<int>)q).ReturnValue;
                    return true;
                };
            }
            query.Tag = orderItem;
            return query;
        }

        private SQLQuery GetOrderQuery(Order order)
        {
            SQLQuery query = (order.PK > 0) ?
                new SQLQuery(SQLUpdateOrder, SQLQueryType.NonQuery) :
                new SQLQueryScaler<int>(SQLInsertOrder);
            query.Parameters.Add("Datetime", order.OrderDatetime);
            query.Parameters.Add("CustomerPK", order.Customer.PK);
            query.Parameters.Add("Status", order.Status);
            if (order.PK > 0)
            {
                query.Parameters.Add("PK", order.PK);
            }
            else
            {
                query.PostQueryProcess = q =>
                {
                    order.PK = ((SQLQueryScaler<int>)q).ReturnValue;
                    return true;
                };
            }
            query.Tag = order;
            return query;
        }
        private Order CreateOrderFromDataReader(DbDataReader reader)
        {
            var order = new Order();
            order.PK = (int)reader["PK"];
            order.OrderDatetime = (DateTime)reader["OrderDatetime"];
            order.Customer = null;
            order.Status = (OrderStatus)reader["Status"];
            return order;
        }
        private OrderItem CreateOrderItemFromDataReader(DbDataReader reader)
        {
            var orderItem = new OrderItem();
            orderItem.PK = (int)reader["PK"];
            orderItem.Order = null;
            orderItem.Quantity = (int)reader["Quantity"];
            orderItem.InventoryID = (int)reader["InventoryID"];
            orderItem.Price = (long)reader["Price"];
            return orderItem;
        }

        public Customer GetCustomerNoOrders(int customerId)
        {
            Customer customer = null;
            var query = new SQLQuery(SQLSelectSingleCustomer);
            query.ProcessRow = dr =>
            {
                customer = CreateCustomerFromDataReader(dr);
                return true;
            };
            query.Parameters.Add("CustomerId", customerId);
            queryRunner.RunQuery(query);
            return customer;
        }
        public Customer UpdateCustomer(Customer customer)
        {
            var query = GetCustomerQuery(customer);
            queryRunner.RunQuery(query);
            return customer;
        }
        public Customer CreateCustomer(Customer customer)
        {
            var query = GetCustomerQuery(customer);
            queryRunner.RunQuery(query);
            return customer;
        }
        private SQLQuery GetCustomerQuery(Customer customer)
        {
            var query = (customer.PK > 0) ?
                new SQLQuery(SQLUpdateSingleCustomer) :
                new SQLQueryScaler<int>(SQLCreateCustomer);
            query.Parameters.Add("Name", customer.Name);
            query.Parameters.Add("EmailAddress", customer.EmailAddress);
            query.Parameters.Add("Password", customer.Password);
            if (customer.PK > 0)
            {
                query.Parameters.Add("CustomerId", customer.PK);
            }
            else
            {
                query.PostQueryProcess = q =>
                {
                    customer.PK = ((SQLQueryScaler<int>)q).ReturnValue;
                    return true;
                };
            }
            return query;
        }
        private Customer CreateCustomerFromDataReader(DbDataReader reader)
        {
            var customer = new Customer();
            customer.PK = (int)reader["PK"];
            customer.Name = (string)reader["Name"];
            customer.EmailAddress = (string)reader["EmailAddress"];
            customer.Password = (string)reader["Password"];
            return customer;
        }
    }
}
