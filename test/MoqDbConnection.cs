using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace test
{
    public abstract class MoqDbConnection : DbConnection, IEnlistmentNotification
    {
        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            return MockDatabaseFactory.DbTransaction.Object;
        }

        private Transaction transaction;
        public override void EnlistTransaction(Transaction transaction)
        {
            RollbackCallCount = 0;
            CommitCallCount = 0;
            InDoubtCallCount = 0;
            PrepareCallCount = 0;

            this.transaction = transaction;
            this.transaction.EnlistVolatile(this, EnlistmentOptions.None);
        }

        public int CommitCallCount { get; private set; }
        public void Commit(Enlistment enlistment)
        {
            CommitCallCount++;
            enlistment.Done();
        }
        public int InDoubtCallCount { get; private set; }
        public void InDoubt(Enlistment enlistment)
        {
            InDoubtCallCount++;
            enlistment.Done();
        }
        public int PrepareCallCount { get; private set; }
        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            PrepareCallCount++;
            preparingEnlistment.Prepared();
        }
        public int RollbackCallCount { get; private set; }
        public void Rollback(Enlistment enlistment)
        {
            RollbackCallCount++;
            enlistment.Done();
        }
    }
}
