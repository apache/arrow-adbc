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
    /// Tag definitions for Error events.
    /// </summary>
    internal static class ErrorEvent
    {
        public const string EventName = "Error";

        // Error Classification
        [TelemetryTag("error.type", ExportScope = TagExportScope.ExportAll, Required = true, Description = "Error type")]
        public const string ErrorType = "error.type";

        [TelemetryTag("http.status_code", ExportScope = TagExportScope.ExportAll, Description = "HTTP status code")]
        public const string HttpStatusCode = "http.status_code";

        [TelemetryTag("db.sql_state", ExportScope = TagExportScope.ExportAll, Description = "SQL state")]
        public const string DbSqlState = "db.sql_state";

        [TelemetryTag("error.operation", ExportScope = TagExportScope.ExportAll, Description = "Failed operation")]
        public const string ErrorOperation = "error.operation";

        [TelemetryTag("error.retried", ExportScope = TagExportScope.ExportAll, Description = "Was retried")]
        public const string ErrorRetried = "error.retried";

        [TelemetryTag("error.retry_count", ExportScope = TagExportScope.ExportAll, Description = "Retry count")]
        public const string ErrorRetryCount = "error.retry_count";

        [TelemetryTag("error.message", ExportScope = TagExportScope.ExportLocal, Description = "Error message")]
        public const string ErrorMessage = "error.message";

        [TelemetryTag("error.stack_trace", ExportScope = TagExportScope.ExportLocal, Description = "Stack trace")]
        public const string ErrorStackTrace = "error.stack_trace";

        /// <summary>
        /// Returns tags allowed for Databricks export (privacy filter).
        /// </summary>
        public static IReadOnlyCollection<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                ErrorType,
                HttpStatusCode,
                DbSqlState,
                ErrorOperation,
                ErrorRetried,
                ErrorRetryCount
            };
        }
    }
}
