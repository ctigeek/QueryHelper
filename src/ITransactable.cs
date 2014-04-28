using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
namespace QueryHelper
{
    public interface ITransactable
    {
        bool TransactionOpen { get; }
        void EnlistTransaction(Transaction transaction);
    }
}
