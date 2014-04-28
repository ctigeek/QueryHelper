using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace test
{
    public abstract class MoqDataReader : DbDataReader
    {
        public virtual Dictionary<string, object> dataRow { get; private set; }
        bool lineRead = false;

        public MoqDataReader(Dictionary<string, object> dataRow)
        {
            this.dataRow = dataRow;
        }

        public override bool Read()
        {
            if (!lineRead)
            {
                lineRead = true;
                return true;
            }
            return false;
        }

        public override object this[string name]
        {
            get
            {
                if (dataRow.ContainsKey(name))
                {
                    return dataRow[name];
                }
                return null;
            }
        }

        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }
    }
}
