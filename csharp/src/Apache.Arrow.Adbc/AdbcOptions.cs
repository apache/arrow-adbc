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

        public const string Autocommit = "adbc.connection.autocommit";
        public const string ReadOnly = "adbc.connection.readonly";
        public const string IsolationLevel = "adbc.connection.transaction.isolation_level";
        public const string CurrentCatalog = "adbc.connection.catalog";
        public const string CurrentDbSchema = "adbc.connection.db_schema";

        public const string Uri = "uri";
        public const string Username = "username";
        public const string Password = "password";

        public static class IsolationLevels
        {
            public const string Default = "adbc.connection.transaction.isolation.default";
            public const string ReadUncommitted = "adbc.connection.transaction.isolation.read_uncommitted";
            public const string ReadCommitted = "adbc.connection.transaction.isolation.read_committed";
            public const string RepeatableRead = "adbc.connection.transaction.isolation.repeatable_read";
            public const string Snapshot = "adbc.connection.transaction.isolation.snapshot";
            public const string Serializable = "adbc.connection.transaction.isolation.serializable";
            public const string Linearizable = "adbc.connection.transaction.isolation.linearizable";
        }

        public static class Ingest
        {
            public const string TargetCatalog = "adbc.ingest.target_catalog";
            public const string TargetDbSchema = "adbc.ingest.target_db_schema";
            public const string TargetTable = "adbc.ingest.target_table";
            public const string Temporary = "adbc.ingest.temporary";
            public const string Mode = "adbc.ingest.mode";
        }

        public static class IngestMode
        {
            public const string Create = "adbc.ingest.mode.create";
            public const string Append = "adbc.ingest.mode.append";
            public const string Replace = "adbc.ingest.mode.replace";
            public const string CreateAppend = "adbc.ingest.mode.create_append";
        }

        public static string GetEnabled(bool value) => value ? Enabled : Disabled;
        public static bool GetEnabled(string value)
        {
            if (StringComparer.OrdinalIgnoreCase.Equals(value, Enabled)) { return true; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, Disabled)) { return false; }
            throw new NotSupportedException("unknown enabled flag");
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
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.Default)) { return Adbc.IsolationLevel.Default; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.ReadUncommitted)) { return Adbc.IsolationLevel.ReadUncommitted; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.ReadCommitted)) { return Adbc.IsolationLevel.ReadCommitted; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.RepeatableRead)) { return Adbc.IsolationLevel.RepeatableRead; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.Snapshot)) { return Adbc.IsolationLevel.Snapshot; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.Serializable)) { return Adbc.IsolationLevel.Serializable; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IsolationLevels.Linearizable)) { return Adbc.IsolationLevel.Linearizable; }
            throw new NotSupportedException("unknown isolation level");
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
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IngestMode.Create)) { return BulkIngestMode.Create; }
            if (StringComparer.OrdinalIgnoreCase.Equals(value, IngestMode.Append)) { return BulkIngestMode.Append; }
            throw new NotSupportedException("unknown ingestion mode");
        }
    }
}
