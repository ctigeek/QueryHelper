using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryHelper
{
    public interface IScalerQuery
    {
        void ProcessScalerResult(object result);
    }
}
