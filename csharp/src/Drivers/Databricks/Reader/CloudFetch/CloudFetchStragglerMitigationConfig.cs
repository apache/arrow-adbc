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
using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Configuration for straggler download mitigation feature.
    /// </summary>
    internal sealed class CloudFetchStragglerMitigationConfig
    {
        /// <summary>
        /// Gets a value indicating whether straggler mitigation is enabled.
        /// </summary>
        public bool Enabled { get; }

        /// <summary>
        /// Gets the straggler throughput multiplier.
        /// A download is considered a straggler if it takes more than (multiplier × expected_time) to complete.
        /// </summary>
        public double Multiplier { get; }

        /// <summary>
        /// Gets the minimum completion quantile before detection starts.
        /// Detection only begins after this fraction of downloads have completed (e.g., 0.6 = 60%).
        /// </summary>
        public double Quantile { get; }

        /// <summary>
        /// Gets the straggler detection padding time.
        /// Extra buffer time added before declaring a download as a straggler.
        /// </summary>
        public TimeSpan Padding { get; }

        /// <summary>
        /// Gets the maximum stragglers allowed before triggering fallback.
        /// </summary>
        public int MaxStragglersBeforeFallback { get; }

        /// <summary>
        /// Gets a value indicating whether synchronous fallback is enabled.
        /// </summary>
        public bool SynchronousFallbackEnabled { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchStragglerMitigationConfig"/> class.
        /// </summary>
        /// <param name="enabled">Whether straggler mitigation is enabled.</param>
        /// <param name="multiplier">Straggler throughput multiplier (default: 1.5).</param>
        /// <param name="quantile">Minimum completion quantile (default: 0.6).</param>
        /// <param name="padding">Straggler detection padding (default: 5 seconds).</param>
        /// <param name="maxStragglersBeforeFallback">Maximum stragglers before fallback (default: 10).</param>
        /// <param name="synchronousFallbackEnabled">Whether synchronous fallback is enabled (default: false).</param>
        public CloudFetchStragglerMitigationConfig(
            bool enabled,
            double multiplier = 1.5,
            double quantile = 0.6,
            TimeSpan? padding = null,
            int maxStragglersBeforeFallback = 10,
            bool synchronousFallbackEnabled = false)
        {
            Enabled = enabled;
            Multiplier = multiplier;
            Quantile = quantile;
            Padding = padding ?? TimeSpan.FromSeconds(5);
            MaxStragglersBeforeFallback = maxStragglersBeforeFallback;
            SynchronousFallbackEnabled = synchronousFallbackEnabled;
        }

        /// <summary>
        /// Gets a disabled configuration (feature off).
        /// </summary>
        public static CloudFetchStragglerMitigationConfig Disabled { get; } =
            new CloudFetchStragglerMitigationConfig(enabled: false);

        /// <summary>
        /// Parses configuration from connection properties.
        /// </summary>
        /// <param name="properties">Connection properties dictionary.</param>
        /// <returns>Parsed configuration, or Disabled if properties is null or feature not enabled.</returns>
        public static CloudFetchStragglerMitigationConfig Parse(
            IReadOnlyDictionary<string, string>? properties)
        {
            if (properties == null)
            {
                return Disabled;
            }

            bool enabled = ParseBooleanProperty(
                properties,
                DatabricksParameters.CloudFetchStragglerMitigationEnabled,
                defaultValue: false);

            if (!enabled)
            {
                return Disabled;
            }

            double multiplier = ParseDoubleProperty(
                properties,
                DatabricksParameters.CloudFetchStragglerMultiplier,
                defaultValue: 1.5);

            double quantile = ParseDoubleProperty(
                properties,
                DatabricksParameters.CloudFetchStragglerQuantile,
                defaultValue: 0.6);

            int paddingSeconds = ParseIntProperty(
                properties,
                DatabricksParameters.CloudFetchStragglerPaddingSeconds,
                defaultValue: 5);

            int maxStragglers = ParseIntProperty(
                properties,
                DatabricksParameters.CloudFetchMaxStragglersPerQuery,
                defaultValue: 10);

            bool synchronousFallback = ParseBooleanProperty(
                properties,
                DatabricksParameters.CloudFetchSynchronousFallbackEnabled,
                defaultValue: false);

            return new CloudFetchStragglerMitigationConfig(
                enabled: true,
                multiplier: multiplier,
                quantile: quantile,
                padding: TimeSpan.FromSeconds(paddingSeconds),
                maxStragglersBeforeFallback: maxStragglers,
                synchronousFallbackEnabled: synchronousFallback);
        }

        // Helper methods for parsing properties
        private static bool ParseBooleanProperty(
            IReadOnlyDictionary<string, string> properties,
            string key,
            bool defaultValue)
        {
            if (properties.TryGetValue(key, out string? value) &&
                bool.TryParse(value, out bool result))
            {
                return result;
            }
            return defaultValue;
        }

        private static int ParseIntProperty(
            IReadOnlyDictionary<string, string> properties,
            string key,
            int defaultValue)
        {
            if (properties.TryGetValue(key, out string? value) &&
                int.TryParse(value,
                    System.Globalization.NumberStyles.Integer,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out int result))
            {
                return result;
            }
            return defaultValue;
        }

        private static double ParseDoubleProperty(
            IReadOnlyDictionary<string, string> properties,
            string key,
            double defaultValue)
        {
            if (properties.TryGetValue(key, out string? value) &&
                double.TryParse(value,
                    System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out double result))
            {
                return result;
            }
            return defaultValue;
        }
    }
}
