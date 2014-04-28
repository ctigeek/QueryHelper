using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace test
{
    public class TestDataContainer
    {
        public Dictionary<string, object> dataRow;

        public TestDataContainer()
        {
            dataRow = new Dictionary<string, object>();
            dataRow.Add("column1", 1);
            dataRow.Add("column2", "3");
            dataRow.Add("column3", DateTime.Parse("1/1/2000"));
        }

        public int column1 = 0;
        public string column2 = string.Empty;
        public DateTime column3 = DateTime.MinValue;

        public void AssertData()
        {
            Assert.AreEqual(1, column1);
            Assert.AreEqual("3", column2);
            Assert.AreEqual(DateTime.Parse("1/1/2000"), column3);
        }

        public Func<DbDataReader, bool> ProcessRow
        {
            get
            {
                return dr =>
                {
                    column1 = (int)dr["column1"];
                    column2 = (string)dr["column2"];
                    column3 = (DateTime)dr["column3"];
                    return true;
                };
            }
        }
    }
}
