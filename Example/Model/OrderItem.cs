using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Example.Model
{
    public class OrderItem
    {
        public int PK { get; set; }
        public Order Order { get; set; }
        public int Quantity { get; set; }
        public int InventoryID { get; set; }
        public long Price { get; set; }
    }
}
