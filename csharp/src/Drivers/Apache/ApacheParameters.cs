﻿/*
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
        public const string IsMetadataCommand = "adbc.apache.statement.is_metadata_command";

        public const string CatalogName = "adbc.apache.catalog_name";
        public const string SchemaName = "adbc.apache.schema_name";
        public const string TableName = "adbc.apache.table_name";
        public const string TableTypes = "adbc.apache.table_types";
        public const string ColumnName = "adbc.apache.column_name";
        public const string ForeignCatalogName = "adbc.apache.foreign_catalog_name";
        public const string ForeignSchemaName = "adbc.apache.foreign_schema_name";
        public const string ForeignTableName = "adbc.apache.foreign_table_name";
    }
}
