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

using System;

namespace Apache.Arrow.Adbc
{
    public static class AdbcOptions
    {
        public const string Enabled = "true";
        public const string Disabled = "false";

        public const string Uri = "uri";
        public const string Username = "username";
        public const string Password = "password";

        public static class Connection
        {
            /// <summary>
            /// The name of the canonical option for whether autocommit is enabled. The type is string.
            /// </summary>
            public const string Autocommit = "adbc.connection.autocommit";

            /// <summary>
            /// The name of the canonical option for whether the current connection should be restricted
            /// to be read-only. The type is string.
            /// </summary>
            public const string ReadOnly = "adbc.connection.readonly";

            /// <summary>
            /// The name of the canonical option for setting the isolation level of a transaction. Should
            /// only be used in conjunction with <see cref="Autocommit">disabled and <see cref="AdbcConnection.Commit"/> /
            /// <see cref="AdbcConnection.Rollback"/>. If the desired isolation level is not supported by a driver, it
            /// should return an appropriate error. The type is string.
            /// </summary>
            public const string IsolationLevel = "adbc.connection.transaction.isolation_level";

            /// <summary>
            /// The name of the canonical option for the current catalog. The type is string.
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string CurrentCatalog = "adbc.connection.catalog";

            /// <summary>
            /// The name of the canonical option for the current schema. The type is string.
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string CurrentDbSchema = "adbc.connection.db_schema";
        }

        public static class Statement
        {
            /// <summary>
            /// The name of the canonical option for making query execution non-blocking. When
            /// enabled, <see cref="AdbcStatement.ExecutePartitioned"/> will return partitions as soon as
            /// they are available instead of returning them all at the end. When there are no more to return,
            /// it will return an empty set of partitions. The type is string.
            ///
            /// <see cref="AdbcStatement.ExecuteQuery"/> and <see cref="AdbcStatement.ExecuteSchema"/> are not
            /// affected. The default is <see cref="Disabled">.
            ///
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string Incremental = "adbc.statement.exec.incremental";

            /// <summary>
            /// The name of the option for getting the progress of a query. The value is not necessarily
            /// in any particular range of have any particular units. For example, it might be a
            /// percentage, bytes of data, rows of data, number of workers, etc. The max value can be
            /// retrieved via <see cref="MaxProgress"/>. The type is double.
            ///
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string Progress = "adbc.statement.exec.progress";

            /// <summary>
            /// The name of the option for getting the maximum progress of a query. This is the value of
            /// <see cref="Progress"/> for a completed query. If not supported or if the value is
            /// nonpositive, then the maximum is not known. For instance, the query may be fully streaming
            /// and the driver does not know when the result set will end. The type is double.
            ///
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string MaxProgress = "adbc.statement.exec.max_progress";
        }

        public static class IsolationLevels
        {
            /// <summary>
            /// Use database or driver default isolation level.
            /// </summary>
            public const string Default = "adbc.connection.transaction.isolation.default";

            /// <summary>
            /// The lowest isolation level. Dirty reads are allowed so one transaction may see not-yet-committed
            /// changes made by others.
            /// </summary>
            public const string ReadUncommitted = "adbc.connection.transaction.isolation.read_uncommitted";

            /// <summary>
            /// Lock-based concurrency control keeps write locks until the end of the transaction, but read
            /// locks are released as soon as a SELECT is performed. Non-repeatable reads can occur with
            /// this isolation level
            /// </summary>
            public const string ReadCommitted = "adbc.connection.transaction.isolation.read_committed";

            /// <summary>
            /// Lock-based concurrency control keeps read and write locks (acquired on selection data)
            /// until the end of the transaction. However, range locks are not managed so phantom reads
            /// can occur. Write skew is possible at this isolation level in some systems.
            /// </summary>
            public const string RepeatableRead = "adbc.connection.transaction.isolation.repeatable_read";

            /// <summary>
            /// This isolation guarantees that all reads in the transaction will see a consistent snapshot
            /// of the database and the transaction should only successfully commit if no updates conflict
            /// with any concurrent updates made since that snapshot.
            /// </summary>
            public const string Snapshot = "adbc.connection.transaction.isolation.snapshot";

            /// <summary>
            /// Serializability requires read and write locks to be released only at the end of the transaction.
            /// This includes acquiring range locks when a select query uses a ranged WHERE clause to avoid
            /// phantom reads.
            /// </summary>
            public const string Serializable = "adbc.connection.transaction.isolation.serializable";

            /// <summary>
            /// Linearizability can be viewed as a special case of strict serializability where transactions
            /// are restricted to consist of a single operation applied to a single object.
            ///
            /// The central distinction between serializability and linearizability is that serializability
            /// is a global property; a property of an entire history of operations and transactions.
            /// Linearizability is a local property; a property of a single operation or transaction.
            /// </summary>
            public const string Linearizable = "adbc.connection.transaction.isolation.linearizable";
        }

        public static class Ingest
        {
            /// <summary>
            /// The catalog of the table for bulk insert. The type is string.
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string TargetCatalog = "adbc.ingest.target_catalog";

            /// <summary>
            /// The schema of the table for bulk insert. The type is string.
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string TargetDbSchema = "adbc.ingest.target_db_schema";

            /// <summary>
            /// The name of the table for bulk insert. The type is string.
            /// </summary>
            public const string TargetTable = "adbc.ingest.target_table";

            /// <summary>
            /// Use a temporary table for ingestion. The type is string.
            /// The value should be enabled or disabled.
            /// </summary>
            public const string Temporary = "adbc.ingest.temporary";

            /// <summary>
            /// Whether to create (the default) or append to the table.
            /// </summary>
            public const string Mode = "adbc.ingest.mode";
        }

        public static class IngestMode
        {
            /// <summary>
            /// Create the table and insert data; error if the table exists.
            /// </summary>
            public const string Create = "adbc.ingest.mode.create";

            /// <summary>
            /// Do not create the table and insert data. Error if the table does exist
            /// (<see cref="AdbcStatusCode.NotFound"/>) or does not match the schema of
            /// the data to append (<see cref="AdbcStatusCode.AlreadyExists"/>).
            /// </summary>
            public const string Append = "adbc.ingest.mode.append";

            /// <summary>
            /// Create the table and insert data; drop the original table if it already exists.
            /// Added as part of API version 1.1.0.
            /// </summary>
            public const string Replace = "adbc.ingest.mode.replace";

            /// <summary>
            /// Insert data; create the table if it does not exist, or error if the table exists
            /// but the schema does not match the schema of the data to append
            /// (<see cref="AdbcStatusCode.AlreadyExists"/>).
            /// </summary>
            public const string CreateAppend = "adbc.ingest.mode.create_append";
        }

        public static string GetEnabled(bool value) => value ? Enabled : Disabled;
        public static bool GetEnabled(string value)
        {
            return value switch
            {
                Enabled => true,
                Disabled => false,
                _ => throw new NotSupportedException("unknown enabled flag"),
            };
        }

        public static string GetIsolationLevel(IsolationLevel value)
        {
            return value switch
            {
                Adbc.IsolationLevel.Default => IsolationLevels.Default,
                Adbc.IsolationLevel.ReadUncommitted => IsolationLevels.ReadUncommitted,
                Adbc.IsolationLevel.ReadCommitted => IsolationLevels.ReadCommitted,
                Adbc.IsolationLevel.RepeatableRead => IsolationLevels.RepeatableRead,
                Adbc.IsolationLevel.Snapshot => IsolationLevels.Snapshot,
                Adbc.IsolationLevel.Serializable => IsolationLevels.Serializable,
                Adbc.IsolationLevel.Linearizable => IsolationLevels.Linearizable,
                _ => throw new NotSupportedException("unknown isolation level"),
            };
        }

        public static IsolationLevel GetIsolationLevel(string value)
        {
            return value switch
            {
                IsolationLevels.Default => Adbc.IsolationLevel.Default,
                IsolationLevels.ReadUncommitted => Adbc.IsolationLevel.ReadUncommitted,
                IsolationLevels.ReadCommitted => Adbc.IsolationLevel.ReadCommitted,
                IsolationLevels.RepeatableRead => Adbc.IsolationLevel.RepeatableRead,
                IsolationLevels.Snapshot => Adbc.IsolationLevel.Snapshot,
                IsolationLevels.Serializable => Adbc.IsolationLevel.Serializable,
                IsolationLevels.Linearizable => Adbc.IsolationLevel.Linearizable,
                _ => throw new NotSupportedException("unknown isolation level"),
            };
        }

        public static string GetIngestMode(BulkIngestMode value)
        {
            return value switch
            {
                BulkIngestMode.Create => IngestMode.Create,
                BulkIngestMode.Append => IngestMode.Append,
                BulkIngestMode.Replace => IngestMode.Replace,
                BulkIngestMode.CreateAppend => IngestMode.CreateAppend,
                _ => throw new NotSupportedException("unknown ingestion mode"),
            };
        }

        public static BulkIngestMode GetIngestMode(string value)
        {
            return value switch
            {
                IngestMode.Create => BulkIngestMode.Create,
                IngestMode.Append => BulkIngestMode.Append,
                _ => throw new NotSupportedException("unknown ingestion mode"),
            };
        }
    }
}
