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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Controls where telemetry tags can be exported.
    /// </summary>
    [Flags]
    internal enum TagExportScope
    {
        None = 0,
        ExportLocal = 1,          // Local diagnostics only
        ExportDatabricks = 2,     // Safe for Databricks service
        ExportAll = ExportLocal | ExportDatabricks
    }

    /// <summary>
    /// Attribute for defining telemetry tags with export controls.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
    internal sealed class TelemetryTagAttribute : Attribute
    {
        public string TagName { get; }
        public TagExportScope ExportScope { get; set; }
        public string? Description { get; set; }
        public bool Required { get; set; }

        public TelemetryTagAttribute(string tagName)
        {
            TagName = tagName ?? throw new ArgumentNullException(nameof(tagName));
            ExportScope = TagExportScope.ExportAll;
        }
    }
}
