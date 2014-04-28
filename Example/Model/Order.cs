using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Example.Model
{
    public enum OrderStatus
    {
        Ordered = 1,
        BackOrdered = 2,
        Shipped = 3,
        Returned = 4,
        Lost = 5,
        Forgotten = 6,
        Destroyed = 7,
        Stolen = 8
    }

    public class Order
    {
        public Order()
        {
            OrderItems = new List<OrderItem>();
        }
        public int PK { get; set; }
        public DateTime OrderDatetime { get; set; }
        public Customer Customer { get; set; }
        public OrderStatus Status { get; set; }

        public List<OrderItem> OrderItems { get; private set; }
    }
}
