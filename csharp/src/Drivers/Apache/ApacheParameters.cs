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

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    /// <summary>
    /// Options common to all Apache drivers.
    /// </summary>
    public class ApacheParameters
    {
        public const string PollTimeMilliseconds = "adbc.apache.statement.polltime_ms";
        public const string BatchSize = "adbc.apache.statement.batch_size";
        public const string QueryTimeoutSeconds = "adbc.apache.statement.query_timeout_s";

        /// <summary>
        /// The indicator of whether the <c>AdbcStatement.ExecuteQuery[Async]</c> should execute a metadata command query.
        /// In the case this indicator is set to <c>True</c>, the method will execute a metadata command using the native API where
        /// the name of the command is given in the <c>AdbcStatement.SqlQuery</c> property value.
        /// <para>
        /// Use the <c>adbc.get_metadata.*</c> options to set the input parameters for the native metadata command query.
        /// </para>
        /// </summary>
        public const string IsMetadataCommand = "adbc.apache.statement.is_metadata_command";

        /// <summary>
        /// The catalog name (or pattern) of the table for GetSchemas, Get* metadata command queries.
        /// </summary>
        public const string CatalogName = "adbc.get_metadata.target_catalog";

        /// <summary>
        /// The schema name (or pattern) of the table for GetSchemas, GetTables, ... metadata command queries.
        /// </summary>
        public const string SchemaName = "adbc.get_metadata.target_db_schema";

        /// <summary>
        /// The table name (or pattern) of the table for GetSchemas, GetTables, ... metadata command queries.
        /// </summary>
        public const string TableName = "adbc.get_metadata.target_table";

        /// <summary>
        /// The comma-separted list of the table types for GetTables metadata command query.
        /// </summary>
        public const string TableTypes = "adbc.get_metadata.target_table_types";

        /// <summary>
        /// The column name (or pattern) in the table for GetColumns metadata command query.
        /// </summary>
        public const string ColumnName = "adbc.get_metadata.target_column";

        /// <summary>
        /// The catalog name (or pattern) of the foreign (child) table for GetCrossReference metadata command query.
        /// </summary>
        public const string ForeignCatalogName = "adbc.get_metadata.foreign_target_catalog";

        /// <summary>
        /// The schema name (or pattern) of the foreign (child) table for GetCrossReference metadata command query.
        /// </summary>
        public const string ForeignSchemaName = "adbc.get_metadata.foreign_target_db_schema";

        /// <summary>
        /// The table name (or pattern) of the foreign (child) table for GetCrossReference metadata command query.
        /// </summary>
        public const string ForeignTableName = "adbc.get_metadata.foreign_target_table";

        /// <summary>
        /// Whether to escape pattern wildcard characters (_ and %) to treat as literal rather than wildcard. Default to false.
        /// </summary>
        public const string EscapePatternWildcards = "adbc.get_metadata.escape_pattern_wildcards";
    }
}
