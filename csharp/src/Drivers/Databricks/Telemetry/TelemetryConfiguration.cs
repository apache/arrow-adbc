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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    /// <summary>
    /// Configuration settings for Databricks telemetry collection and export.
    /// </summary>
    public sealed class TelemetryConfiguration
    {
        public bool Enabled { get; set; } = true;

        public int BatchSize { get; set; } = 100;

        public int FlushIntervalMs { get; set; } = 5000;

        public int MaxRetries { get; set; } = 3;

        public int RetryDelayMs { get; set; } = 100;

        public bool CircuitBreakerEnabled { get; set; } = true;

        public int CircuitBreakerThreshold { get; set; } = 5;

        public TimeSpan CircuitBreakerTimeout { get; set; } = TimeSpan.FromMinutes(1);
    }
}
