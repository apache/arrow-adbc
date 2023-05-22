using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Core
{
    /// <summary>
    /// The isolation level to use for transactions when autocommit is disabled.
    /// </summary>
    public enum IsolationLevel
    {
        Default,

        /// <summary>
        /// The lowest isolation level. Dirty reads are allowed, so one transaction may see not-yet-committed changes made by others.
        /// </summary>
        ReadUncommitted,

        /// <summary>
        /// Lock-based concurrency control keeps write locks until the end of the transaction, but read
        /// locks are released as soon as a SELECT is performed. Non-repeatable reads can occur in this
        /// isolation level.
        /// </summary>
        ReadCommitted,

        /// <summary>
        /// Lock-based concurrency control keeps read AND write locks (acquired on selection data) until
        /// the end of the transaction.
        /// </summary>
        RepeatableRead,

        /**
         * This isolation guarantees that all reads in the transaction will see a consistent snapshot of
         * the database and the transaction should only successfully commit if no updates conflict with
         * any concurrent updates made since that snapshot.
         */
        Snapshot,
        /**
         * Serializability requires read and write locks to be released only at the end of the
         * transaction. This includes acquiring range- locks when a select query uses a ranged WHERE
         * clause to avoid phantom reads.
         */
        Serializable,
        /**
         * The central distinction between serializability and linearizability is that serializability is
         * a global property; a property of an entire history of operations and transactions.
         * Linearizability is a local property; a property of a single operation/transaction.
         *
         * <p>Linearizability can be viewed as a special case of strict serializability where transactions
         * are restricted to consist of a single operation applied to a single object.
         */
        Linearizable,
    }

}
