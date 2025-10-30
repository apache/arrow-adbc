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

using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Tag definitions for Statement execution events.
    /// </summary>
    internal static class StatementExecutionEvent
    {
        public const string EventName = "Statement.Execute";

        // Identity
        [TelemetryTag("statement.id", ExportScope = TagExportScope.ExportDatabricks, Required = true, Description = "Statement ID")]
        public const string StatementId = "statement.id";

        [TelemetryTag("session.id", ExportScope = TagExportScope.ExportDatabricks, Required = true, Description = "Session ID")]
        public const string SessionId = "session.id";

        // Result Metrics
        [TelemetryTag("result.format", ExportScope = TagExportScope.ExportDatabricks, Description = "Result format")]
        public const string ResultFormat = "result.format";

        [TelemetryTag("result.chunk_count", ExportScope = TagExportScope.ExportDatabricks, Description = "Chunk count")]
        public const string ResultChunkCount = "result.chunk_count";

        [TelemetryTag("result.bytes_downloaded", ExportScope = TagExportScope.ExportDatabricks, Description = "Bytes downloaded")]
        public const string ResultBytesDownloaded = "result.bytes_downloaded";

        [TelemetryTag("result.compression_enabled", ExportScope = TagExportScope.ExportDatabricks, Description = "Compression enabled")]
        public const string ResultCompressionEnabled = "result.compression_enabled";

        [TelemetryTag("result.row_count", ExportScope = TagExportScope.ExportDatabricks, Description = "Row count")]
        public const string ResultRowCount = "result.row_count";

        // Polling Metrics
        [TelemetryTag("poll.count", ExportScope = TagExportScope.ExportDatabricks, Description = "Poll count")]
        public const string PollCount = "poll.count";

        [TelemetryTag("poll.latency_ms", ExportScope = TagExportScope.ExportDatabricks, Description = "Poll latency")]
        public const string PollLatencyMs = "poll.latency_ms";

        // Operation Type
        [TelemetryTag("db.operation", ExportScope = TagExportScope.ExportDatabricks, Description = "Operation type")]
        public const string DbOperation = "db.operation";

        [TelemetryTag("db.statement", ExportScope = TagExportScope.ExportLocal, Description = "SQL statement")]
        public const string DbStatement = "db.statement";

        [TelemetryTag("db.catalog", ExportScope = TagExportScope.ExportLocal, Description = "Catalog name")]
        public const string DbCatalog = "db.catalog";

        [TelemetryTag("db.schema", ExportScope = TagExportScope.ExportLocal, Description = "Schema name")]
        public const string DbSchema = "db.schema";

        public static IReadOnlyCollection<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                StatementId,
                SessionId,
                ResultFormat,
                ResultChunkCount,
                ResultBytesDownloaded,
                ResultCompressionEnabled,
                ResultRowCount,
                PollCount,
                PollLatencyMs,
                DbOperation
            };
        }
    }
}
