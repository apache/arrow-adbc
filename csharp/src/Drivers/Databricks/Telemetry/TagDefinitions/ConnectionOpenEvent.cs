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
    /// Tag definitions for Connection.Open events.
    /// </summary>
    internal static class ConnectionOpenEvent
    {
        public const string EventName = "Connection.Open";

        // Identity
        [TelemetryTag("workspace.id", ExportScope = TagExportScope.ExportAll, Required = true, Description = "Workspace ID")]
        public const string WorkspaceId = "workspace.id";

        [TelemetryTag("session.id", ExportScope = TagExportScope.ExportAll, Required = true, Description = "Session ID")]
        public const string SessionId = "session.id";

        // Driver Configuration
        [TelemetryTag("driver.version", ExportScope = TagExportScope.ExportAll, Description = "Driver version")]
        public const string DriverVersion = "driver.version";

        [TelemetryTag("driver.os", ExportScope = TagExportScope.ExportAll, Description = "Operating system")]
        public const string DriverOS = "driver.os";

        [TelemetryTag("driver.runtime", ExportScope = TagExportScope.ExportAll, Description = ".NET runtime")]
        public const string DriverRuntime = "driver.runtime";

        // Feature Flags
        [TelemetryTag("feature.cloudfetch", ExportScope = TagExportScope.ExportAll, Description = "CloudFetch enabled")]
        public const string FeatureCloudFetch = "feature.cloudfetch";

        [TelemetryTag("feature.lz4", ExportScope = TagExportScope.ExportAll, Description = "LZ4 compression enabled")]
        public const string FeatureLz4 = "feature.lz4";

        [TelemetryTag("feature.direct_results", ExportScope = TagExportScope.ExportAll, Description = "Direct results enabled")]
        public const string FeatureDirectResults = "feature.direct_results";

        [TelemetryTag("feature.multiple_catalog", ExportScope = TagExportScope.ExportAll, Description = "Multiple catalog enabled")]
        public const string FeatureMultipleCatalog = "feature.multiple_catalog";

        [TelemetryTag("feature.trace_propagation", ExportScope = TagExportScope.ExportAll, Description = "Trace propagation enabled")]
        public const string FeatureTracePropagation = "feature.trace_propagation";

        [TelemetryTag("server.address", ExportScope = TagExportScope.ExportLocal, Description = "Server address")]
        public const string ServerAddress = "server.address";

        /// <summary>
        /// Returns tags allowed for Databricks export (privacy filter).
        /// </summary>
        public static IReadOnlyCollection<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                WorkspaceId,
                SessionId,
                DriverVersion,
                DriverOS,
                DriverRuntime,
                FeatureCloudFetch,
                FeatureLz4,
                FeatureDirectResults,
                FeatureMultipleCatalog,
                FeatureTracePropagation
            };
        }
    }
}
