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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;
using System.Linq;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    public class StatementTests : StatementTests<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public StatementTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [InlineData(true, "CloudFetch enabled")]
        [InlineData(false, "CloudFetch disabled")]
        public async Task LZ4DecompressionCapabilityTest(bool useCloudFetch, string configName)
        {
            OutputHelper?.WriteLine($"Testing with LZ4 decompression capability enabled ({configName})");

            // Create a connection using the test configuration
            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();

            // Set options for LZ4 decompression (enabled by default) and CloudFetch as specified
            statement.SetOption(DatabricksParameters.UseCloudFetch, useCloudFetch.ToString().ToLower());
            OutputHelper?.WriteLine($"CloudFetch is {(useCloudFetch ? "enabled" : "disabled")}");
            OutputHelper?.WriteLine("LZ4 decompression capability is enabled by default");

            // Execute a query that should return data
            statement.SqlQuery = "SELECT id, CAST(id AS STRING) as id_string, id * 2 as id_doubled FROM RANGE(100)";
            QueryResult result = statement.ExecuteQuery();

            // Verify we have a valid stream
            Assert.NotNull(result.Stream);

            // Read all batches
            int totalRows = 0;
            int batchCount = 0;

            while (result.Stream != null)
            {
                using var batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                    break;

                batchCount++;
                totalRows += batch.Length;
                OutputHelper?.WriteLine($"Batch {batchCount}: Read {batch.Length} rows");
            }

            // Verify we got all rows
            Assert.Equal(100, totalRows);
            OutputHelper?.WriteLine($"Successfully read {totalRows} rows in {batchCount} batches with {configName}");
            OutputHelper?.WriteLine("NOTE: Whether actual LZ4 compression was used is determined by the server");
        }


        [SkippableTheory]
        [ClassData(typeof(LongRunningStatementTimeoutTestData))]
        internal override void StatementTimeoutTest(StatementWithExceptions statementWithExceptions)
        {
            base.StatementTimeoutTest(statementWithExceptions);
        }

        internal class LongRunningStatementTimeoutTestData : ShortRunningStatementTimeoutTestData
        {
            public LongRunningStatementTimeoutTestData()
            {
                string longRunningQuery = "SELECT COUNT(*) AS total_count\nFROM (\n  SELECT t1.id AS id1, t2.id AS id2\n  FROM RANGE(1000000) t1\n  CROSS JOIN RANGE(100000) t2\n) subquery\nWHERE MOD(id1 + id2, 2) = 0";

                Add(new(5, longRunningQuery, typeof(TimeoutException)));
                Add(new(null, longRunningQuery, typeof(TimeoutException)));
                Add(new(0, longRunningQuery, null));
            }
        }

        [SkippableFact]
        public async Task CanGetPrimaryKeysDatabricks()
        {
            await base.CanGetPrimaryKeys(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceFromParentTableDatabricks()
        {
            await base.CanGetCrossReferenceFromParentTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceFromChildTableDatabricks()
        {
            await base.CanGetCrossReferenceFromChildTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetColumnsWithBaseTypeName()
        {
            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
            statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
            statement.SetOption(ApacheParameters.TableName, TestConfiguration.Metadata.Table);
            statement.SqlQuery = "GetColumns";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            // We should have 24 columns now (the original 23 + BASE_TYPE_NAME)
            Assert.Equal(24, queryResult.Stream.Schema.FieldsList.Count);

            // Verify the BASE_TYPE_NAME column is present
            bool hasBaseTypeNameColumn = false;
            int baseTypeNameIndex = -1;
            int typeNameIndex = -1;

            for (int i = 0; i < queryResult.Stream.Schema.FieldsList.Count; i++)
            {
                if (queryResult.Stream.Schema.FieldsList[i].Name.Equals("BASE_TYPE_NAME", StringComparison.OrdinalIgnoreCase))
                {
                    hasBaseTypeNameColumn = true;
                    baseTypeNameIndex = i;
                }
                else if (queryResult.Stream.Schema.FieldsList[i].Name.Equals("TYPE_NAME", StringComparison.OrdinalIgnoreCase))
                {
                    typeNameIndex = i;
                }
            }

            Assert.True(hasBaseTypeNameColumn, "BASE_TYPE_NAME column not found in GetColumns result");
            Assert.True(typeNameIndex >= 0, "TYPE_NAME column not found in GetColumns result");

            // Read batches and verify BASE_TYPE_NAME values
            int actualBatchLength = 0;

            // Track if we've seen specific complex types
            bool foundDecimal = false;
            bool foundInterval = false;
            bool foundMap = false;
            bool foundArray = false;
            bool foundStruct = false;

            Dictionary<string, string> typeNameToBaseTypeName = new Dictionary<string, string>();

            // For tracking decimal precision and scale
            int columnSizeIndex = -1;
            int decimalDigitsIndex = -1;
            Dictionary<string, (int precision, short scale)> decimalTypeInfo = new Dictionary<string, (int, short)>();

            // Find COLUMN_SIZE and DECIMAL_DIGITS columns
            for (int i = 0; i < queryResult.Stream.Schema.FieldsList.Count; i++)
            {
                if (queryResult.Stream.Schema.FieldsList[i].Name.Equals("COLUMN_SIZE", StringComparison.OrdinalIgnoreCase))
                {
                    columnSizeIndex = i;
                }
                else if (queryResult.Stream.Schema.FieldsList[i].Name.Equals("DECIMAL_DIGITS", StringComparison.OrdinalIgnoreCase))
                {
                    decimalDigitsIndex = i;
                }

                if (columnSizeIndex >= 0 && decimalDigitsIndex >= 0)
                    break;
            }

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                actualBatchLength += batch.Length;

                // Verify relationships between TYPE_NAME and BASE_TYPE_NAME for each row
                for (int i = 0; i < batch.Length; i++)
                {
                    string? typeName = ((StringArray)batch.Column(typeNameIndex)).GetString(i);
                    string? baseTypeName = ((StringArray)batch.Column(baseTypeNameIndex)).GetString(i);

                    // Store for later analysis
                    if (!string.IsNullOrEmpty(typeName) && !string.IsNullOrEmpty(baseTypeName))
                    {
                        typeNameToBaseTypeName[typeName] = baseTypeName;

                        // Collect precision and scale for DECIMAL types
                        if (typeName.StartsWith("DECIMAL(") && columnSizeIndex >= 0 && decimalDigitsIndex >= 0)
                        {
                            int? precision = ((Int32Array)batch.Column(columnSizeIndex)).GetValue(i);
                            int? scale = ((Int32Array)batch.Column(decimalDigitsIndex)).GetValue(i);

                            if (precision.HasValue && scale.HasValue)
                            {
                                decimalTypeInfo[typeName] = (precision.Value, (short)scale.Value);
                            }
                        }

                        // Track if we've found specific complex types
                        if (typeName.StartsWith("DECIMAL("))
                            foundDecimal = true;
                        else if (typeName.StartsWith("INTERVAL"))
                            foundInterval = true;
                        else if (typeName.StartsWith("MAP<"))
                            foundMap = true;
                        else if (typeName.StartsWith("ARRAY<"))
                            foundArray = true;
                        else if (typeName.StartsWith("STRUCT<"))
                            foundStruct = true;
                    }

                    // BASE_TYPE_NAME should not be null if TYPE_NAME is not null
                    if (!string.IsNullOrEmpty(typeName))
                    {
                        Assert.NotNull(baseTypeName);

                        // BASE_TYPE_NAME should be contained within TYPE_NAME or equal to it
                        // But we might have cases like "ARRAY<INT>" where baseTypeName would be "ARRAY"
                        if (!typeName.Contains("<") && !typeName.Contains("(") && !typeName.Contains(" "))
                        {
                            // Simple types should match exactly, with special handling for INT vs INTEGER
                            bool isEquivalentType =
                                typeName == baseTypeName ||
                                ((typeName == "INT" && baseTypeName == "INTEGER")) ||
                                ((typeName == "TIMESTAMP_NTZ" || typeName == "TIMESTAMP_LTZ") && baseTypeName == "TIMESTAMP");

                            Assert.True(isEquivalentType,
                                $"TypeName '{typeName}' should be equivalent to BaseTypeName '{baseTypeName}'");
                        }
                        else
                        {
                            // Complex types should have BASE_TYPE_NAME as a prefix (without parameters)
                            Assert.True(typeName.StartsWith(baseTypeName),
                                $"TypeName '{typeName}' should start with BaseTypeName '{baseTypeName}'");

                            // The BASE_TYPE_NAME should not contain angle brackets or parentheses
                            Assert.DoesNotContain("(", baseTypeName);
                            Assert.DoesNotContain("<", baseTypeName);
                        }

                        OutputHelper?.WriteLine($"TYPE_NAME: {typeName}, BASE_TYPE_NAME: {baseTypeName}");
                    }
                }
            }

            // Specific tests for complex types - if we found them in the results
            if (foundDecimal)
            {
                string decimalTypeName = typeNameToBaseTypeName.Keys.First(k => k.StartsWith("DECIMAL("));
                string decimalBaseTypeName = typeNameToBaseTypeName[decimalTypeName];
                Assert.Equal("DECIMAL", decimalBaseTypeName);
                OutputHelper?.WriteLine($"Verified DECIMAL: {decimalTypeName} -> {decimalBaseTypeName}");

                // Extract precision and scale from the type name (e.g., "DECIMAL(38,10)" -> precision=38, scale=10)
                string typePart = decimalTypeName.Substring(decimalTypeName.IndexOf('(') + 1);
                typePart = typePart.Remove(typePart.Length - 1); // Remove closing parenthesis
                string[] parts = typePart.Split(',');

                int expectedPrecision = int.Parse(parts[0]);
                int expectedScale = parts.Length > 1 ? int.Parse(parts[1]) : 0;

                // Verify that the precision and scale from the data match what's in the type name
                Assert.True(decimalTypeInfo.ContainsKey(decimalTypeName),
                    $"Could not find precision and scale information for {decimalTypeName}");

                var (actualPrecision, actualScale) = decimalTypeInfo[decimalTypeName];
                Assert.Equal(expectedPrecision, actualPrecision);
                Assert.Equal(expectedScale, actualScale);

                OutputHelper?.WriteLine($"Verified DECIMAL precision/scale: {decimalTypeName} -> precision={actualPrecision}, scale={actualScale}");
            }

            if (foundInterval)
            {
                string intervalTypeName = typeNameToBaseTypeName.Keys.First(k => k.StartsWith("INTERVAL"));
                string intervalBaseTypeName = typeNameToBaseTypeName[intervalTypeName];
                Assert.Equal("INTERVAL", intervalBaseTypeName);
                OutputHelper?.WriteLine($"Verified INTERVAL: {intervalTypeName} -> {intervalBaseTypeName}");
            }

            if (foundMap)
            {
                string mapTypeName = typeNameToBaseTypeName.Keys.First(k => k.StartsWith("MAP<"));
                string mapBaseTypeName = typeNameToBaseTypeName[mapTypeName];
                Assert.Equal("MAP", mapBaseTypeName);
                OutputHelper?.WriteLine($"Verified MAP: {mapTypeName} -> {mapBaseTypeName}");
            }

            if (foundArray)
            {
                string arrayTypeName = typeNameToBaseTypeName.Keys.First(k => k.StartsWith("ARRAY<"));
                string arrayBaseTypeName = typeNameToBaseTypeName[arrayTypeName];
                Assert.Equal("ARRAY", arrayBaseTypeName);
                OutputHelper?.WriteLine($"Verified ARRAY: {arrayTypeName} -> {arrayBaseTypeName}");
            }

            if (foundStruct)
            {
                string structTypeName = typeNameToBaseTypeName.Keys.First(k => k.StartsWith("STRUCT<"));
                string structBaseTypeName = typeNameToBaseTypeName[structTypeName];
                Assert.Equal("STRUCT", structBaseTypeName);
                OutputHelper?.WriteLine($"Verified STRUCT: {structTypeName} -> {structBaseTypeName}");
            }

            Assert.Equal(TestConfiguration.Metadata.ExpectedColumnCount, actualBatchLength);
        }

        [SkippableFact]
        public async Task CanGetColumnsExtended()
        {
            // Get the runtime version using GetInfo
            var infoCodes = new List<AdbcInfoCode> { AdbcInfoCode.VendorVersion };
            var infoValues = Connection.GetInfo(infoCodes);

            // Set up statement for GetColumnsExtended
            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
            statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
            statement.SetOption(ApacheParameters.TableName, TestConfiguration.Metadata.Table);
            statement.SqlQuery = "GetColumnsExtended";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            // Verify schema has more fields than the regular GetColumns result (which has 24 fields)
            // We expect additional PK and FK fields
            OutputHelper?.WriteLine($"Column count in result schema: {queryResult.Stream.Schema.FieldsList.Count}");
            Assert.True(queryResult.Stream.Schema.FieldsList.Count > 24,
                "GetColumnsExtended should return more columns than GetColumns (at least 24+)");

            // Verify that key fields from each original metadata call are present
            bool hasColumnName = false;
            bool hasPkKeySeq = false;
            bool hasFkTableName = false;

            foreach (var field in queryResult.Stream.Schema.FieldsList)
            {
                OutputHelper?.WriteLine($"Field in schema: {field.Name} ({field.DataType})");

                if (field.Name.Equals("COLUMN_NAME", StringComparison.OrdinalIgnoreCase))
                    hasColumnName = true;
                else if (field.Name.Equals("PK_COLUMN_NAME", StringComparison.OrdinalIgnoreCase))
                    hasPkKeySeq = true;
                else if (field.Name.Equals("FK_PKTABLE_NAME", StringComparison.OrdinalIgnoreCase))
                    hasFkTableName = true;
            }

            Assert.True(hasColumnName, "Schema should contain COLUMN_NAME field from GetColumns");
            Assert.True(hasPkKeySeq, "Schema should contain PK_KEY_SEQ field from GetPrimaryKeys");
            Assert.True(hasFkTableName, "Schema should contain FK_PKTABLE_NAME field from GetCrossReference");

            // Read and verify data
            int rowCount = 0;
            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null) break;

                rowCount += batch.Length;

                // Output rows for debugging (limit to first 10)
                if (batch.Length > 0)
                {
                    int rowsToPrint = Math.Min(batch.Length, 10); // Limit to 10 rows
                    OutputHelper?.WriteLine($"Found {batch.Length} rows, showing first {rowsToPrint}:");

                    for (int rowIndex = 0; rowIndex < rowsToPrint; rowIndex++)
                    {
                        OutputHelper?.WriteLine($"Row {rowIndex}:");
                        for (int i = 0; i < batch.ColumnCount; i++)
                        {
                            string fieldName = queryResult.Stream.Schema.FieldsList[i].Name;
                            string fieldValue = GetStringValue(batch.Column(i), rowIndex);
                            OutputHelper?.WriteLine($"  {fieldName}: {fieldValue}");
                        }
                        OutputHelper?.WriteLine(""); // Add blank line between rows
                    }
                }
            }

            // Verify we got rows matching the expected column count
            Assert.Equal(TestConfiguration.Metadata.ExpectedColumnCount, rowCount);
            OutputHelper?.WriteLine($"Successfully retrieved {rowCount} columns with extended information");
        }

        // Helper method to get string representation of array values
        private string GetStringValue(IArrowArray array, int index)
        {
            if (array == null || index >= array.Length || array.IsNull(index))
                return "null";

            if (array is StringArray strArray)
                return strArray.GetString(index) ?? "null";
            else if (array is Int32Array int32Array)
                return int32Array.GetValue(index).ToString() ?? "null";
            else if (array is Int16Array int16Array)
                return int16Array.GetValue(index).ToString() ?? "null";
            else if (array is BooleanArray boolArray)
                return boolArray.GetValue(index).ToString() ?? "null";

            return "unknown";
        }

        protected override void PrepareCreateTableWithPrimaryKeys(out string sqlUpdate, out string tableNameParent, out string fullTableNameParent, out IReadOnlyList<string> primaryKeys)
        {
            CreateNewTableName(out tableNameParent, out fullTableNameParent);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameParent} (INDEX INT, NAME STRING, PRIMARY KEY (INDEX, NAME))";
            primaryKeys = ["index", "name"];
        }
    }
}
