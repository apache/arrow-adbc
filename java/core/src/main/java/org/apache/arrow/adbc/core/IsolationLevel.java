/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adbc.core;

/** The isolation level to use for transactions when autocommit is disabled. */
public enum IsolationLevel {
  /** The database or driver default isolation level. */
  DEFAULT,
  /**
   * The lowest isolation level. Dirty reads are allowed, so one transaction may see
   * not-yet-committed changes made by others.
   */
  READ_UNCOMMITTED,
  /**
   * Lock-based concurrency control keeps write locks until the end of the transaction, but read
   * locks are released as soon as a SELECT is performed. Non-repeatable reads can occur in this
   * isolation level.
   *
   * <p>More simply put, Read Committed is an isolation level that guarantees that any data read is
   * committed at the moment it is read. It simply restricts the reader from seeing any
   * intermediate, uncommitted, 'dirty' reads. It makes no promise whatsoever that if the
   * transaction re-issues the read, it will find the same data; data is free to change after it is
   * read.
   */
  READ_COMMITTED,
  /**
   * Lock-based concurrency control keeps read AND write locks (acquired on selection data) until
   * the end of the transaction.
   *
   * <p>However, range-locks are not managed, so phantom reads can occur. Write skew is possible at
   * this isolation level in some systems.
   */
  REPEATABLE_READ,
  /**
   * This isolation guarantees that all reads in the transaction will see a consistent snapshot of
   * the database and the transaction should only successfully commit if no updates conflict with
   * any concurrent updates made since that snapshot.
   */
  SNAPSHOT,
  /**
   * Serializability requires read and write locks to be released only at the end of the
   * transaction. This includes acquiring range- locks when a select query uses a ranged WHERE
   * clause to avoid phantom reads.
   */
  SERIALIZABLE,
  /**
   * The central distinction between serializability and linearizability is that serializability is
   * a global property; a property of an entire history of operations and transactions.
   * Linearizability is a local property; a property of a single operation/transaction.
   *
   * <p>Linearizability can be viewed as a special case of strict serializability where transactions
   * are restricted to consist of a single operation applied to a single object.
   */
  LINEARIZABLE,
}
