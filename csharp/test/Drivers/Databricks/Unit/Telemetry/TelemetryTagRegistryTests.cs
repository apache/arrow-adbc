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

using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.TagDefinitions;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Telemetry
{
    public class TelemetryTagRegistryTests
    {
        [Theory]
        [InlineData(TelemetryEventType.StatementExecution, "db.statement")]
        [InlineData(TelemetryEventType.ConnectionOpen, "server.address")]
        [InlineData(TelemetryEventType.Error, "error.message")]
        public void ShouldExportToDatabricks_SensitiveTags_ReturnsFalse(TelemetryEventType eventType, string tagName)
        {
            var shouldExport = TelemetryTagRegistry.ShouldExportToDatabricks(eventType, tagName);
            Assert.False(shouldExport);
        }

        [Theory]
        [InlineData(TelemetryEventType.ConnectionOpen, "workspace.id")]
        [InlineData(TelemetryEventType.StatementExecution, "result.chunk_count")]
        [InlineData(TelemetryEventType.Error, "error.type")]
        public void ShouldExportToDatabricks_NonSensitiveTags_ReturnsTrue(TelemetryEventType eventType, string tagName)
        {
            var shouldExport = TelemetryTagRegistry.ShouldExportToDatabricks(eventType, tagName);
            Assert.True(shouldExport);
        }

        [Fact]
        public void GetDatabricksExportTags_UnknownEventType_ReturnsEmpty()
        {
            var tags = TelemetryTagRegistry.GetDatabricksExportTags((TelemetryEventType)999);
            Assert.Empty(tags);
        }
    }
}
