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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Defines the types of telemetry events that can be emitted by the driver.
    /// Each event type has its own set of allowed tags defined in corresponding *Event classes.
    /// </summary>
    public enum TelemetryEventType
    {
        /// <summary>
        /// Connection open event. Emitted when a connection is established.
        /// Tags defined in: <see cref="ConnectionOpenEvent"/>
        /// </summary>
        ConnectionOpen,

        /// <summary>
        /// Statement execution event. Emitted when a query or statement is executed.
        /// Tags defined in: <see cref="StatementExecutionEvent"/>
        /// </summary>
        StatementExecution,

        /// <summary>
        /// Error event. Emitted when an error occurs during any operation.
        /// Tags defined in: <see cref="ErrorEvent"/>
        /// </summary>
        Error
    }
}
