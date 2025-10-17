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

using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// Unit tests for Statement Execution API parameters and constants.
    /// </summary>
    public class StatementExecutionParametersTest
    {
        /// <summary>
        /// Tests that all Statement Execution API parameter keys are defined.
        /// </summary>
        [Fact]
        public void Parameters_AllKeysAreDefined()
        {
            // Assert - Verify all parameter constants are defined
            Assert.Equal("adbc.databricks.protocol", DatabricksParameters.Protocol);
            Assert.Equal("adbc.databricks.result_disposition", DatabricksParameters.ResultDisposition);
            Assert.Equal("adbc.databricks.result_format", DatabricksParameters.ResultFormat);
            Assert.Equal("adbc.databricks.result_compression", DatabricksParameters.ResultCompression);
            Assert.Equal("adbc.databricks.wait_timeout", DatabricksParameters.WaitTimeout);
            Assert.Equal("adbc.databricks.polling_interval_ms", DatabricksParameters.PollingInterval);
        }

        /// <summary>
        /// Tests that protocol type constants have correct values.
        /// </summary>
        [Theory]
        [InlineData("thrift")]
        [InlineData("rest")]
        public void ProtocolTypes_HaveCorrectValues(string expectedValue)
        {
            // Act & Assert
            if (expectedValue == "thrift")
                Assert.Equal(DatabricksConstants.ProtocolTypes.Thrift, expectedValue);
            else if (expectedValue == "rest")
                Assert.Equal(DatabricksConstants.ProtocolTypes.Rest, expectedValue);
        }

        /// <summary>
        /// Tests that result disposition constants have correct values.
        /// </summary>
        [Theory]
        [InlineData("inline")]
        [InlineData("external_links")]
        [InlineData("inline_or_external_links")]
        public void ResultDispositions_HaveCorrectValues(string expectedValue)
        {
            // Act & Assert
            switch (expectedValue)
            {
                case "inline":
                    Assert.Equal(DatabricksConstants.ResultDispositions.Inline, expectedValue);
                    break;
                case "external_links":
                    Assert.Equal(DatabricksConstants.ResultDispositions.ExternalLinks, expectedValue);
                    break;
                case "inline_or_external_links":
                    Assert.Equal(DatabricksConstants.ResultDispositions.InlineOrExternalLinks, expectedValue);
                    break;
            }
        }

        /// <summary>
        /// Tests that result format constants have correct values.
        /// </summary>
        [Theory]
        [InlineData("arrow_stream")]
        [InlineData("json_array")]
        [InlineData("csv")]
        public void ResultFormats_HaveCorrectValues(string expectedValue)
        {
            // Act & Assert
            switch (expectedValue)
            {
                case "arrow_stream":
                    Assert.Equal(DatabricksConstants.ResultFormats.ArrowStream, expectedValue);
                    break;
                case "json_array":
                    Assert.Equal(DatabricksConstants.ResultFormats.JsonArray, expectedValue);
                    break;
                case "csv":
                    Assert.Equal(DatabricksConstants.ResultFormats.Csv, expectedValue);
                    break;
            }
        }

        /// <summary>
        /// Tests that result compression constants have correct values.
        /// </summary>
        [Theory]
        [InlineData("lz4")]
        [InlineData("gzip")]
        [InlineData("none")]
        public void ResultCompressions_HaveCorrectValues(string expectedValue)
        {
            // Act & Assert
            switch (expectedValue)
            {
                case "lz4":
                    Assert.Equal(DatabricksConstants.ResultCompressions.Lz4, expectedValue);
                    break;
                case "gzip":
                    Assert.Equal(DatabricksConstants.ResultCompressions.Gzip, expectedValue);
                    break;
                case "none":
                    Assert.Equal(DatabricksConstants.ResultCompressions.None, expectedValue);
                    break;
            }
        }

        /// <summary>
        /// Tests that default values are set correctly.
        /// </summary>
        [Fact]
        public void DefaultValues_AreCorrect()
        {
            // Assert
            Assert.Equal(10, DatabricksConstants.DefaultWaitTimeoutSeconds);
            Assert.Equal(1000, DatabricksConstants.DefaultPollingIntervalMs);
        }

        /// <summary>
        /// Tests that parameter keys do not conflict with existing parameters.
        /// </summary>
        [Fact]
        public void Parameters_DoNotConflictWithExisting()
        {
            // Assert - Verify new parameters don't accidentally override existing ones
            Assert.NotEqual(DatabricksParameters.Protocol, DatabricksParameters.EnableDirectResults);
            Assert.NotEqual(DatabricksParameters.WaitTimeout, DatabricksParameters.FetchHeartbeatInterval);
            Assert.NotEqual(DatabricksParameters.PollingInterval, DatabricksParameters.FetchHeartbeatInterval);
        }

        /// <summary>
        /// Tests that all parameter keys follow the naming convention.
        /// </summary>
        [Fact]
        public void Parameters_FollowNamingConvention()
        {
            // Assert - All parameters should start with "adbc.databricks."
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.Protocol);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.ResultDisposition);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.ResultFormat);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.ResultCompression);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.WaitTimeout);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.PollingInterval);
        }

        /// <summary>
        /// Tests that constant classes are accessible and not null.
        /// </summary>
        [Fact]
        public void ConstantClasses_AreAccessible()
        {
            // Assert - Verify all constant classes can be accessed
            Assert.NotNull(DatabricksConstants.ProtocolTypes.Thrift);
            Assert.NotNull(DatabricksConstants.ProtocolTypes.Rest);
            Assert.NotNull(DatabricksConstants.ResultDispositions.Inline);
            Assert.NotNull(DatabricksConstants.ResultDispositions.ExternalLinks);
            Assert.NotNull(DatabricksConstants.ResultDispositions.InlineOrExternalLinks);
            Assert.NotNull(DatabricksConstants.ResultFormats.ArrowStream);
            Assert.NotNull(DatabricksConstants.ResultFormats.JsonArray);
            Assert.NotNull(DatabricksConstants.ResultFormats.Csv);
            Assert.NotNull(DatabricksConstants.ResultCompressions.Lz4);
            Assert.NotNull(DatabricksConstants.ResultCompressions.Gzip);
            Assert.NotNull(DatabricksConstants.ResultCompressions.None);
        }
    }
}
