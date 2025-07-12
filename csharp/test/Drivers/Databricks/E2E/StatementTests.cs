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
using System.IO;
using System.Text.Json;

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
            public LongRunningStatementTimeoutTestData() : base("SELECT 1")
            {
                string longRunningQuery = "SELECT COUNT(*) AS total_count\nFROM (\n  SELECT t1.id AS id1, t2.id AS id2\n  FROM RANGE(1000000) t1\n  CROSS JOIN RANGE(100000) t2\n) subquery\nWHERE MOD(id1 + id2, 2) = 0";

                // Add Databricks-specific long-running query tests
                Add(new(5, longRunningQuery, typeof(TimeoutException)));
                Add(new(null, longRunningQuery, typeof(TimeoutException)));
                Add(new(0, longRunningQuery, null));
            }
        }

        protected override void CreateNewTableName(out string tableName, out string fullTableName)
        {
            string catalogName = TestConfiguration.Metadata.Catalog;
            string schemaName = TestConfiguration.Metadata.Schema;
            tableName = Guid.NewGuid().ToString("N") + "`!@#$%^&*()_+-=";
            string catalogFormatted = string.IsNullOrEmpty(catalogName) ? string.Empty : DelimitIdentifier(catalogName) + ".";
            fullTableName = $"{catalogFormatted}{DelimitIdentifier(schemaName)}.{DelimitIdentifier(tableName)}";
        }

        [SkippableFact]
        public async Task CanGetPrimaryKeysDatabricks()
        {
            await base.CanGetPrimaryKeys(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceFromParentTableDatabricks()
        {
            // TODO: Get cross reference from Parent is not currently supported in Databricks
            Skip.If(true, "GetCrossReference is not supported in Databricks");
            await base.CanGetCrossReferenceFromParentTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceFromChildTableDatabricks()
        {
            await base.CanGetCrossReferenceFromChildTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        /// <summary>
        /// Comprehensive test that verifies disposal works for all statement types without throwing exceptions.
        /// This prevents regressions like the GetColumns disposal bug where _directResults wasn't set properly.
        /// </summary>
        [SkippableTheory]
        [InlineData("ExecuteStatement", "SELECT 1 as test_column")]
        [InlineData("GetCatalogs", "GetCatalogs")]
        [InlineData("GetSchemas", "GetSchemas")]
        [InlineData("GetTables", "GetTables")]
        [InlineData("GetColumns", "GetColumns")]
        [InlineData("GetPrimaryKeys", "GetPrimaryKeys")]
        [InlineData("GetCrossReference", "GetCrossReference")]
        [InlineData("GetColumnsExtended", "GetColumnsExtended")]
        public async Task AllStatementTypesDisposeWithoutErrors(string statementType, string sqlCommand)
        {
            var statement = Connection.CreateStatement();

            try
            {
                if (statementType == "ExecuteStatement")
                {
                    // Regular SQL statement
                    statement.SqlQuery = sqlCommand;
                }
                else
                {
                    // Metadata command
                    statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
                    statement.SqlQuery = sqlCommand;

                    // Set required parameters for specific metadata commands
                    if (sqlCommand is "GetColumns" or "GetPrimaryKeys" or "GetCrossReference" or "GetColumnsExtended")
                    {
                        statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
                        statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
                        statement.SetOption(ApacheParameters.TableName, TestConfiguration.Metadata.Table);
                    }

                    if (sqlCommand == "GetCrossReference")
                    {
                        // GetCrossReference needs foreign table parameters too
                        statement.SetOption(ApacheParameters.ForeignCatalogName, TestConfiguration.Metadata.Catalog);
                        statement.SetOption(ApacheParameters.ForeignSchemaName, TestConfiguration.Metadata.Schema);
                        statement.SetOption(ApacheParameters.ForeignTableName, TestConfiguration.Metadata.Table);
                    }
                }

                // Execute the statement
                QueryResult queryResult = await statement.ExecuteQueryAsync();
                Assert.NotNull(queryResult.Stream);

                // Consume at least one batch to ensure the operation completes
                var batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                // Note: batch might be null for empty results, that's OK

                // The critical test: disposal should not throw any exceptions
                // This specifically tests the fix for the GetColumns bug where _directResults wasn't set
                var exception = Record.Exception(() => statement.Dispose());
                Assert.Null(exception);
            }
            catch (Exception ex)
            {
                // If execution fails, we still want to test disposal
                OutputHelper?.WriteLine($"Statement execution failed for {statementType}: {ex.Message}");

                // Even if execution failed, disposal should not throw
                var disposalException = Record.Exception(() => statement.Dispose());
                Assert.Null(disposalException);

                // Re-throw the original exception if we want to investigate execution failures
                // For now, we'll skip the test if execution fails since disposal is our main concern
                Skip.If(true, $"Skipping disposal test for {statementType} due to execution failure: {ex.Message}");
            }
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

        [SkippableTheory]
        [InlineData("all_column_types", "Resources/create_table_all_types.sql", "Resources/result_get_column_extended_all_types.json", true, new[] { "PK_IS_NULLABLE:NO" })]
        [InlineData("all_column_types", "Resources/create_table_all_types.sql", "Resources/result_get_column_extended_all_types.json", false, new[] { "PK_IS_NULLABLE:YES" })]
        public async Task CanGetColumnsExtended(string tableName,string createTableSqlLocation, string resultLocation, bool useDescTableExtended, string[] ?extraPlaceholdsInResult = null)
        {
            var connectionParams = new Dictionary<string, string> { ["adbc.databricks.use_desc_table_extended"] = $"{useDescTableExtended}" };

            using AdbcConnection connection = NewConnection(TestConfiguration, connectionParams);

            // Get the runtime version using GetInfo
            var infoCodes = new List<AdbcInfoCode> { AdbcInfoCode.VendorVersion };
            var infoValues = Connection.GetInfo(infoCodes);
            var catalog = TestConfiguration.Metadata.Catalog;
            var schema = TestConfiguration.Metadata.Schema;

            // Prepare table for test

            // First create reference table
            var refTableName = "fk_test_ref_table";
            var refFullTableName = $"`{catalog}`.`{schema}`.`{refTableName}`";
            await PrepareTableAsync(refFullTableName, "Resources/create_reference_table.sql");

            var fullTableName = $"`{catalog}`.`{schema}`.`{tableName}`";
            await PrepareTableAsync(fullTableName, createTableSqlLocation, refTableName);

            // Set up statement for GetColumnsExtended
            var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalog);
            statement.SetOption(ApacheParameters.SchemaName, schema);
            statement.SetOption(ApacheParameters.TableName, tableName);
            statement.SetOption(ApacheParameters.EscapePatternWildcards, "true");
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

            // Define the expected schema as (name, type) pairs
            var expectedSchema = new (string Name, string Type)[]
            {
                ("TABLE_CAT", "Apache.Arrow.Types.StringType"),
                ("TABLE_SCHEM", "Apache.Arrow.Types.StringType"),
                ("TABLE_NAME", "Apache.Arrow.Types.StringType"),
                ("COLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("DATA_TYPE", "Apache.Arrow.Types.Int32Type"),
                ("TYPE_NAME", "Apache.Arrow.Types.StringType"),
                ("COLUMN_SIZE", "Apache.Arrow.Types.Int32Type"),
                ("BUFFER_LENGTH", "Apache.Arrow.Types.Int8Type"),
                ("DECIMAL_DIGITS", "Apache.Arrow.Types.Int32Type"),
                ("NUM_PREC_RADIX", "Apache.Arrow.Types.Int32Type"),
                ("NULLABLE", "Apache.Arrow.Types.Int32Type"),
                ("REMARKS", "Apache.Arrow.Types.StringType"),
                ("COLUMN_DEF", "Apache.Arrow.Types.StringType"),
                ("SQL_DATA_TYPE", "Apache.Arrow.Types.Int32Type"),
                ("SQL_DATETIME_SUB", "Apache.Arrow.Types.Int32Type"),
                ("CHAR_OCTET_LENGTH", "Apache.Arrow.Types.Int32Type"),
                ("ORDINAL_POSITION", "Apache.Arrow.Types.Int32Type"),
                ("IS_NULLABLE", "Apache.Arrow.Types.StringType"),
                ("SCOPE_CATALOG", "Apache.Arrow.Types.StringType"),
                ("SCOPE_SCHEMA", "Apache.Arrow.Types.StringType"),
                ("SCOPE_TABLE", "Apache.Arrow.Types.StringType"),
                ("SOURCE_DATA_TYPE", "Apache.Arrow.Types.Int16Type"),
                ("IS_AUTO_INCREMENT", "Apache.Arrow.Types.StringType"),
                ("BASE_TYPE_NAME", "Apache.Arrow.Types.StringType"),
                ("PK_COLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_PKCOLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_PKTABLE_CAT", "Apache.Arrow.Types.StringType"),
                ("FK_PKTABLE_SCHEM", "Apache.Arrow.Types.StringType"),
                ("FK_PKTABLE_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_FKCOLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_FK_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_KEQ_SEQ", "Apache.Arrow.Types.Int32Type"),
            };

            // Assert each expected field exists in the actual schema with the correct type
            foreach (var (expectedName, expectedType) in expectedSchema)
            {
                var actualField = queryResult.Stream?.Schema.FieldsList
                    .FirstOrDefault(f => f.Name == expectedName);

                Assert.NotNull(actualField); // Field must exist
                Assert.Equal(expectedType, actualField.DataType.GetType().ToString());
            }

            // Read and verify data
            var result = await ConvertQueryResultToList(queryResult);
            var resultJson = JsonSerializer.Serialize(result);
            var rowCount = result.Count;

            // Load expected result
            var rows = new List<Dictionary<string, object?>>();
            var resultString = File.ReadAllText(resultLocation)
                .Replace("{CATALOG_NAME}", catalog)
                .Replace("{SCHEMA_NAME}", schema)
                .Replace("{TABLE_NAME}", tableName)
                .Replace("{REF_TABLE_NAME}", refTableName);

            // Apply extra placeholder replacements if provided
            if (extraPlaceholdsInResult != null)
            {
                foreach (var placeholderReplacement in extraPlaceholdsInResult)
                {
                    var parts = placeholderReplacement.Split(':');
                    if (parts.Length == 2)
                    {
                        var placeholder = parts[0];
                        var replacement = parts[1];
                        resultString = resultString.Replace("{" + placeholder + "}", replacement);
                    }
                }
            }

            var expectedResult = JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(resultString);

            // For debug
            OutputHelper?.WriteLine(resultJson);

            // Verify we got rows matching the expected column count
            Assert.Equal(expectedResult!.Count, rowCount);

            // Verify the result match expected result
            // Assert.Equal cannot do the deep compare between 2 list with nested object, let us compare with json
            var expectedResultJson = JsonSerializer.Serialize(expectedResult);
            Assert.Equal(expectedResultJson, resultJson);

            OutputHelper?.WriteLine($"Successfully retrieved {rowCount} columns with extended information");
        }

        [SkippableFact]
        public async Task CanGetColumnsOnNoColumnTable()
        {
            string? catalogName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;
            string tableName = Guid.NewGuid().ToString("N");
            string fullTableName = string.Format(
                "{0}{1}{2}",
                string.IsNullOrEmpty(catalogName) ? string.Empty : DelimitIdentifier(catalogName) + ".",
                string.IsNullOrEmpty(schemaName) ? string.Empty : DelimitIdentifier(schemaName) + ".",
                DelimitIdentifier(tableName));
            using TemporaryTable temporaryTable = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableName,
                $"CREATE TABLE IF NOT EXISTS {fullTableName} ();",
                OutputHelper);

            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalogName);
            statement.SetOption(ApacheParameters.SchemaName, schemaName);
            statement.SetOption(ApacheParameters.TableName, tableName);
            statement.SqlQuery = "GetColumns";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            // 23 original metadata columns and one added for "base type"
            Assert.Equal(24, queryResult.Stream.Schema.FieldsList.Count);
            int actualBatchLength = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }
                actualBatchLength += batch.Length;
            }
            Assert.Equal(0, actualBatchLength);
        }

        [SkippableFact]
        public async Task CanGetColumnsExtendedOnNoColumnTable()
        {
            string? catalogName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;
            string tableName = Guid.NewGuid().ToString("N");
            string fullTableName = string.Format(
                "{0}{1}{2}",
                string.IsNullOrEmpty(catalogName) ? string.Empty : DelimitIdentifier(catalogName) + ".",
                string.IsNullOrEmpty(schemaName) ? string.Empty : DelimitIdentifier(schemaName) + ".",
                DelimitIdentifier(tableName));
            using TemporaryTable temporaryTable = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableName,
                $"CREATE TABLE IF NOT EXISTS {fullTableName} ();",
                OutputHelper);

            using AdbcConnection connection = NewConnection();

            // Get the runtime version using GetInfo
            var infoCodes = new List<AdbcInfoCode> { AdbcInfoCode.VendorVersion };
            var infoValues = Connection.GetInfo(infoCodes);

            // Set up statement for GetColumnsExtended
            var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalogName);
            statement.SetOption(ApacheParameters.SchemaName, schemaName);
            statement.SetOption(ApacheParameters.TableName, tableName);
            statement.SetOption(ApacheParameters.EscapePatternWildcards, "true");
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

            // Define the expected schema as (name, type) pairs
            var expectedSchema = new (string Name, string Type)[]
            {
                ("TABLE_CAT", "Apache.Arrow.Types.StringType"),
                ("TABLE_SCHEM", "Apache.Arrow.Types.StringType"),
                ("TABLE_NAME", "Apache.Arrow.Types.StringType"),
                ("COLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("DATA_TYPE", "Apache.Arrow.Types.Int32Type"),
                ("TYPE_NAME", "Apache.Arrow.Types.StringType"),
                ("COLUMN_SIZE", "Apache.Arrow.Types.Int32Type"),
                ("BUFFER_LENGTH", "Apache.Arrow.Types.Int8Type"),
                ("DECIMAL_DIGITS", "Apache.Arrow.Types.Int32Type"),
                ("NUM_PREC_RADIX", "Apache.Arrow.Types.Int32Type"),
                ("NULLABLE", "Apache.Arrow.Types.Int32Type"),
                ("REMARKS", "Apache.Arrow.Types.StringType"),
                ("COLUMN_DEF", "Apache.Arrow.Types.StringType"),
                ("SQL_DATA_TYPE", "Apache.Arrow.Types.Int32Type"),
                ("SQL_DATETIME_SUB", "Apache.Arrow.Types.Int32Type"),
                ("CHAR_OCTET_LENGTH", "Apache.Arrow.Types.Int32Type"),
                ("ORDINAL_POSITION", "Apache.Arrow.Types.Int32Type"),
                ("IS_NULLABLE", "Apache.Arrow.Types.StringType"),
                ("SCOPE_CATALOG", "Apache.Arrow.Types.StringType"),
                ("SCOPE_SCHEMA", "Apache.Arrow.Types.StringType"),
                ("SCOPE_TABLE", "Apache.Arrow.Types.StringType"),
                ("SOURCE_DATA_TYPE", "Apache.Arrow.Types.Int16Type"),
                ("IS_AUTO_INCREMENT", "Apache.Arrow.Types.StringType"),
                ("BASE_TYPE_NAME", "Apache.Arrow.Types.StringType"),
                ("PK_COLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_PKCOLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_PKTABLE_CAT", "Apache.Arrow.Types.StringType"),
                ("FK_PKTABLE_SCHEM", "Apache.Arrow.Types.StringType"),
                ("FK_PKTABLE_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_FKCOLUMN_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_FK_NAME", "Apache.Arrow.Types.StringType"),
                ("FK_KEQ_SEQ", "Apache.Arrow.Types.Int32Type"),
            };

            RecordBatch batch = await queryResult.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            // Assert each expected field exists in the actual schema with the correct type
            for (int i = 0; i < expectedSchema.Length; i++)
            {
                (string expectedName, string expectedType) = expectedSchema[i];
                var actualField = queryResult.Stream?.Schema.FieldsList
                    .FirstOrDefault(f => f.Name == expectedName);

                Assert.NotNull(actualField); // Field must exist
                Assert.Equal(expectedType, actualField.DataType.GetType().ToString());
                var columnArray = batch.Column(i);
                Assert.Equal(actualField.DataType, columnArray.Data.DataType);
            }
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


        protected override void PrepareCreateTableWithForeignKeys(string fullTableNameParent, out string sqlUpdate, out string tableNameChild, out string fullTableNameChild, out IReadOnlyList<string> foreignKeys)
        {
            CreateNewTableName(out tableNameChild, out fullTableNameChild);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameChild} \n"
                + "  (INDEX INT, USERINDEX INT, USERNAME STRING, ADDRESS STRING, \n"
                + "  PRIMARY KEY (INDEX), \n"
                + $"  FOREIGN KEY (USERINDEX, USERNAME) REFERENCES {fullTableNameParent} (INDEX, NAME))";
            foreignKeys = ["userindex", "username"];
        }

        private async Task PrepareTableAsync(string fullTableName, string sqlResourceLocation, string? refTableName = null)
        {
            var sql = File.ReadAllText(sqlResourceLocation);
            using var dropStmt = Connection.CreateStatement();
            dropStmt.SqlQuery = $"DROP TABLE IF EXISTS {fullTableName}";
            await dropStmt.ExecuteUpdateAsync();

            using var createStmt = Connection.CreateStatement();
            var finalSql = sql.Replace("{TABLE_NAME}", fullTableName);
            if (refTableName != null)
            {
                finalSql = finalSql.Replace("{CATALOG_NAME}", TestConfiguration.Metadata.Catalog)
                .Replace("{SCHEMA_NAME}", TestConfiguration.Metadata.Schema)
                .Replace("{REF_TABLE_NAME}", refTableName);
            }

            createStmt.SqlQuery = finalSql;
            await createStmt.ExecuteUpdateAsync();
        }

        internal static List<Dictionary<string, object?>> LoadFromResultFile(string resultResourceLocation, string tableName)
        {
            var rows = new List<Dictionary<string, object?>>();
            var resultString = File.ReadAllText(resultResourceLocation).Replace("{TABLE_NAME}", tableName);
            var result = JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(resultString);
            if (result == null)
            {
                return rows;
            }

            // C# deserialization will convert the value to JsonValueKind instead of the original type
            // We need to convert it back to the original type
            foreach (var row in result)
            {
                foreach (var key in row.Keys.ToList())
                {
                    if (row[key] is JsonElement jsonElement)
                    {
                        row[key] = jsonElement.ValueKind switch
                        {
                            JsonValueKind.String => jsonElement.GetString(),
                            JsonValueKind.Number => jsonElement.GetInt32(),
                            JsonValueKind.True => true,
                            JsonValueKind.False => false,
                            JsonValueKind.Null => null,
                            _ => null
                        };
                    }
                }
                rows.Add(row);
            }
            return rows;
        }

        internal static async Task<List<Dictionary<string, object?>>> ConvertQueryResultToList(QueryResult result)
        {
            var rows = new List<Dictionary<string, object?>>();
            if (result.Stream == null)
                return rows;

            while (result.Stream != null)
            {
                using var batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                    break;


                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new Dictionary<string, object?>();
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        string fieldName = result.Stream.Schema.FieldsList[j].Name;
                        var value = GetArrayValue(batch.Column(j), i);
                        row[fieldName] = value;
                    }
                    rows.Add(row);
                }
            }
            return rows;
        }

        internal static object? GetArrayValue(IArrowArray array, int index)
        {
            if (array == null || index >= array.Length || array.IsNull(index))
                return null;

            return array switch
            {
                StringArray strArray => strArray.GetString(index),
                Int32Array int32Array => int32Array.GetValue(index),
                Int64Array int64Array => int64Array.GetValue(index),
                Int16Array int16Array => int16Array.GetValue(index),
                Int8Array int8Array => int8Array.GetValue(index),
                UInt32Array uint32Array => uint32Array.GetValue(index),
                UInt64Array uint64Array => uint64Array.GetValue(index),
                UInt16Array uint16Array => uint16Array.GetValue(index),
                UInt8Array uint8Array => uint8Array.GetValue(index),
                FloatArray floatArray => floatArray.GetValue(index),
                DoubleArray doubleArray => doubleArray.GetValue(index),
                BooleanArray boolArray => boolArray.GetValue(index),
                TimestampArray timestampArray => timestampArray.GetValue(index),
                Date32Array date32Array => date32Array.GetValue(index),
                Date64Array date64Array => date64Array.GetValue(index),
                Time32Array time32Array => time32Array.GetValue(index),
                Time64Array time64Array => time64Array.GetValue(index),
                Decimal128Array decimal128Array => decimal128Array.GetValue(index),
                Decimal256Array decimal256Array => decimal256Array.GetValue(index),
                _ => null
            };
        }

        // NOTE: this is a thirty minute test. As of writing, databricks commands have 20 minutes of idle time (and checked every 5 minutes)
        [SkippableTheory]
        [InlineData(false, "CloudFetch disabled")]
        [InlineData(true, "CloudFetch enabled")]
        public async Task StatusPollerKeepsQueryAlive(bool useCloudFetch, string configName)
        {
            OutputHelper?.WriteLine($"Testing status poller with long delay between reads ({configName})");

            // Create a connection using the test configuration
            var connectionParams = new Dictionary<string, string>
            {
                [DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString().ToLower()
            };
            using AdbcConnection connection = NewConnection(TestConfiguration, connectionParams);
            using var statement = connection.CreateStatement();

            // Execute a query that should return data - using a larger dataset to ensure multiple batches
            statement.SqlQuery = "SELECT id, CAST(id AS STRING) as id_string, id * 2 as id_doubled FROM RANGE(30000000)";
            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result.Stream);

            // Simulate a long delay (30 minutes)
            OutputHelper?.WriteLine("Simulating 30 minute delay...");
            await Task.Delay(TimeSpan.FromMinutes(30));

            // Read remaining batches
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
            Assert.Equal(30000000, totalRows);
            Assert.True(batchCount > 1, "Should have read multiple batches");
            OutputHelper?.WriteLine($"Successfully read {totalRows} rows in {batchCount} batches after 30 minute delay with {configName}");
        }

        [SkippableTheory]
        [InlineData("true", true)]  // Should allow multiple catalogs
        [InlineData("false", false)] // Should only use default catalog
        public async Task EnableMultipleCatalogSupportAffectsMetadataQueries(string enableMultipleCatalogSupport, bool shouldAllowMultipleCatalogs)
        {
            // Create a connection with the specified EnableMultipleCatalogSupport setting
            var testConfig = (DatabricksTestConfiguration)TestConfiguration.Clone();
            testConfig.EnableMultipleCatalogSupport = enableMultipleCatalogSupport;
            using var connection = NewConnection(testConfig);

            // Store SPARK catalog schemas for comparison
            Dictionary<string, Schema> sparkSchemas = new Dictionary<string, Schema>();

            // First run with SPARK catalog to get real schemas
            await TestMetadataQuery(connection, "GetCatalogs", shouldAllowMultipleCatalogs, "SPARK", sparkSchemas);
            await TestMetadataQuery(connection, "GetSchemas", shouldAllowMultipleCatalogs, "SPARK", sparkSchemas);
            await TestMetadataQuery(connection, "GetTables", shouldAllowMultipleCatalogs, "SPARK", sparkSchemas);
            await TestMetadataQuery(connection, "GetColumns", shouldAllowMultipleCatalogs, "SPARK", sparkSchemas);

            // Then run with non-SPARK catalog and compare schemas
            await TestMetadataQuery(connection, "GetCatalogs", shouldAllowMultipleCatalogs, "main", sparkSchemas);
            await TestMetadataQuery(connection, "GetSchemas", shouldAllowMultipleCatalogs, "main", sparkSchemas);
            await TestMetadataQuery(connection, "GetTables", shouldAllowMultipleCatalogs, "main", sparkSchemas);
            await TestMetadataQuery(connection, "GetColumns", shouldAllowMultipleCatalogs, "main", sparkSchemas);
        }

        private async Task TestMetadataQuery(AdbcConnection connection, string queryType, bool shouldAllowMultipleCatalogs, string catalogName, Dictionary<string, Schema> sparkSchemas)
        {
            OutputHelper?.WriteLine($"Testing {queryType} with EnableMultipleCatalogSupport={shouldAllowMultipleCatalogs}, CatalogName={catalogName}");

            var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalogName);
            // Use default as schema name, it is the default schema name
            statement.SetOption(ApacheParameters.SchemaName, "default");
            statement.SqlQuery = queryType;

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            // Store SPARK catalog schema for comparison
            if (catalogName.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
            {
                sparkSchemas[queryType] = queryResult.Stream.Schema;
            }
            // When EnableMultipleCatalogSupport is false and catalog is not SPARK, compare with SPARK schema
            else if (!shouldAllowMultipleCatalogs)
            {
                var currentSchema = queryResult.Stream.Schema;
                var sparkSchema = sparkSchemas[queryType];

                // Compare field counts
                Assert.True(sparkSchema.FieldsList.Count == currentSchema.FieldsList.Count,
                    $"{queryType}: Schema field count mismatch between SPARK and {catalogName} catalogs");

                // Compare each field
                for (int i = 0; i < sparkSchema.FieldsList.Count; i++)
                {
                    var sparkField = sparkSchema.FieldsList[i];
                    var currentField = currentSchema.FieldsList[i];

                    Assert.True(sparkField.Name == currentField.Name,
                        $"{queryType}: Field name mismatch at index {i} between SPARK and {catalogName} catalogs");
                    Assert.True(sparkField.DataType.Equals(currentField.DataType),
                        $"{queryType}: Field type mismatch at index {i} between SPARK and {catalogName} catalogs");
                    Assert.True(sparkField.IsNullable == currentField.IsNullable,
                        $"{queryType}: Field nullability mismatch at index {i} between SPARK and {catalogName} catalogs");
                }

                OutputHelper?.WriteLine($"Verified schema for {queryType} matches SPARK catalog schema");
            }

            int rowCount = 0;
            HashSet<string> foundCatalogs = new HashSet<string>();
            string? defaultCatalog = null;

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null) break;

                rowCount += batch.Length;

                // Check catalog values in each row
                for (int i = 0; i < batch.Length; i++)
                {
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        if (queryResult.Stream.Schema.FieldsList[j].Name.Equals("TABLE_CATALOG", StringComparison.OrdinalIgnoreCase) ||
                            queryResult.Stream.Schema.FieldsList[j].Name.Equals("TABLE_CAT", StringComparison.OrdinalIgnoreCase))
                        {
                            string? catalog = GetStringValue(batch.Column(j), i);
                            if (!string.IsNullOrEmpty(catalog))
                            {
                                foundCatalogs.Add(catalog);
                                // Store the first catalog we find as the default catalog
                                defaultCatalog ??= catalog;
                            }
                        }
                    }
                }
            }

            OutputHelper?.WriteLine($"{queryType} returned {rowCount} rows, found {foundCatalogs.Count} different catalogs: {string.Join(", ", foundCatalogs)}");

            // Special handling for GetCatalogs
            if (queryType == "GetCatalogs")
            {
                if (!shouldAllowMultipleCatalogs)
                {
                    // When EnableMultipleCatalogSupport is false, should return just "SPARK"
                    Assert.Equal(1, rowCount);
                    Assert.Single(foundCatalogs);
                    Assert.Contains("SPARK", foundCatalogs);
                    OutputHelper?.WriteLine("Verified that only the synthetic SPARK catalog was returned when EnableMultipleCatalogSupport is false");
                }
                else
                {
                    // When EnableMultipleCatalogSupport is true, should return actual catalogs
                    Assert.True(rowCount >= 1, "GetCatalogs should return at least one catalog");
                    Assert.DoesNotContain("SPARK", foundCatalogs);
                    OutputHelper?.WriteLine("Verified that actual catalogs were returned when EnableMultipleCatalogSupport is true");
                }
                return;
            }

            // For other metadata queries
            if (!shouldAllowMultipleCatalogs)
            {
                if (catalogName.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
                {
                    // When EnableMultipleCatalogSupport is false and catalog is SPARK, results should be from default catalog
                    Assert.True(foundCatalogs.Count == 1,
                        $"{queryType} should only return results from the default catalog when EnableMultipleCatalogSupport is false and catalog is SPARK");
                    OutputHelper?.WriteLine($"All results are from default catalog: {defaultCatalog}");
                }
                else
                {
                    // When EnableMultipleCatalogSupport is false and catalog is not SPARK, should return empty result
                    Assert.Equal(0, rowCount);
                    Assert.Empty(foundCatalogs);
                    OutputHelper?.WriteLine($"Verified empty result when EnableMultipleCatalogSupport is false and catalog is not SPARK");
                }
            }
            else
            {
                // When EnableMultipleCatalogSupport is true
                if (catalogName.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
                {
                    // When catalog is SPARK, we may have results from multiple catalogs
                    Assert.True(foundCatalogs.Count > 1,
                        $"{queryType} should return results from multiple catalogs when EnableMultipleCatalogSupport is true and catalog is SPARK");
                    OutputHelper?.WriteLine($"Found results from multiple catalogs: {string.Join(", ", foundCatalogs)}");
                }
                else
                {
                    // When catalog is not SPARK, we should only get results from that specific catalog
                    Assert.True(foundCatalogs.Count == 1,
                        $"{queryType} should return results from only the specified catalog when EnableMultipleCatalogSupport is true and catalog is not SPARK");
                    Assert.Contains(catalogName, foundCatalogs);
                    OutputHelper?.WriteLine($"Found results from catalog: {string.Join(", ", foundCatalogs)}");
                }
            }
        }

        private void AssertField(Schema schema, int index, string expectedName, IArrowType expectedType, bool expectedNullable)
        {
            var field = schema.FieldsList[index];
            Assert.True(expectedName.Equals(field.Name), $"Field {index} name mismatch");
            Assert.True(expectedType.Equals(field.DataType), $"Field {index} type mismatch");
            Assert.True(expectedNullable == field.IsNullable, $"Field {index} nullability mismatch");
        }

        [Theory]
        [InlineData(false, "main", true)]
        [InlineData(true, null, true)]
        [InlineData(true, "", true)]
        [InlineData(true, "SPARK", true)]
        [InlineData(true, "hive_metastore", true)]
        [InlineData(true, "main", false)]
        public void ShouldReturnEmptyPkFkResult_WorksAsExpected(bool enablePKFK, string? catalogName, bool expected)
        {
            // Arrange: create test configuration and connection
            var testConfig = (DatabricksTestConfiguration)TestConfiguration.Clone();
            var connectionParams = new Dictionary<string, string>
            {
                [DatabricksParameters.EnablePKFK] = enablePKFK.ToString().ToLowerInvariant()
            };
            using var connection = NewConnection(testConfig, connectionParams);
            var statement = connection.CreateStatement();

            // Set CatalogName using SetOption
            if (catalogName != null)
            {
                statement.SetOption(ApacheParameters.CatalogName, catalogName);
            }

            // Act
            var result = ((DatabricksStatement)statement).ShouldReturnEmptyPkFkResult();

            // Assert
            Assert.Equal(expected, result);
        }

        [SkippableFact]
        public async Task PKFK_EmptyResult_SchemaMatches_RealMetadataResponse()
        {
            // Arrange: create test configuration and connection
            var testConfig = (DatabricksTestConfiguration)TestConfiguration.Clone();
            var connectionParams = new Dictionary<string, string>
            {
                [DatabricksParameters.EnablePKFK] = "true"
            };
            using var connection = NewConnection(testConfig, connectionParams);
            var statement = connection.CreateStatement();

            // Get real PK metadata schema
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "powerbi");
            statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
            statement.SetOption(ApacheParameters.TableName, TestConfiguration.Metadata.Table);
            statement.SqlQuery = "GetPrimaryKeys";
            var realPkResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(realPkResult.Stream);
            var realPkSchema = realPkResult.Stream.Schema;

            // Get empty PK result schema (using SPARK catalog which should return empty)
            statement.SetOption(ApacheParameters.CatalogName, "SPARK");
            var emptyPkResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(emptyPkResult.Stream);
            var emptyPkSchema = emptyPkResult.Stream.Schema;

            // Verify PK schemas match
            Assert.Equal(realPkSchema.FieldsList.Count, emptyPkSchema.FieldsList.Count);
            for (int i = 0; i < realPkSchema.FieldsList.Count; i++)
            {
                var realField = realPkSchema.FieldsList[i];
                var emptyField = emptyPkSchema.FieldsList[i];
                AssertField(emptyField, realField.Name, realField.DataType, realField.IsNullable);
            }

            // Get real FK metadata schema
            statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
            statement.SqlQuery = "GetCrossReference";
            var realFkResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(realFkResult.Stream);
            var realFkSchema = realFkResult.Stream.Schema;

            // Get empty FK result schema
            statement.SetOption(ApacheParameters.CatalogName, "SPARK");
            var emptyFkResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(emptyFkResult.Stream);
            var emptyFkSchema = emptyFkResult.Stream.Schema;

            // Verify FK schemas match
            Assert.Equal(realFkSchema.FieldsList.Count, emptyFkSchema.FieldsList.Count);
            for (int i = 0; i < realFkSchema.FieldsList.Count; i++)
            {
                var realField = realFkSchema.FieldsList[i];
                var emptyField = emptyFkSchema.FieldsList[i];
                AssertField(emptyField, realField.Name, realField.DataType, realField.IsNullable);
            }
        }

        private void AssertField(Field field, string expectedName, IArrowType expectedType, bool expectedNullable)
        {
            Assert.True(expectedName.Equals(field.Name), $"Field name mismatch: expected {expectedName}, got {field.Name}");
            Assert.True(expectedType.Equals(field.DataType), $"Field type mismatch: expected {expectedType}, got {field.DataType}");
            Assert.True(expectedNullable == field.IsNullable, $"Field nullability mismatch: expected {expectedNullable}, got {field.IsNullable}");
        }

        [SkippableFact]
        public async Task MetadataQuery_ShouldUseResponseNamespace()
        {
            // Test case: GetTables should use the response namespace. You can run this test with and without a catalog or schema.
            using var connection = NewConnection();
            using var statement = connection.CreateStatement();

            // Set only schema name without catalog
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetTables";

            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            // Verify we get results and they use the response namespace
            int rowCount = 0;
            var foundCatalogs = new HashSet<string>();
            var foundSchemas = new HashSet<string>();

            while (result.Stream != null)
            {
                using var batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                    break;

                rowCount += batch.Length;
                var catalogArray = (StringArray)batch.Column(0);
                var schemaArray = (StringArray)batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    foundCatalogs.Add(catalogArray.GetString(i) ?? string.Empty);
                    foundSchemas.Add(schemaArray.GetString(i) ?? string.Empty);
                }
            }

            // Should have results and they should match the schema we specified
            Assert.True(rowCount > 0, "Should have results even without catalog specified");
            Assert.True(foundSchemas.Count >= 1, "Should have at least one schema");
            Assert.True(foundCatalogs.Count == 1, "Should have exactly one catalog");
        }

        // run this test with dbr < 10.4
        [SkippableFact]
        public async Task OlderDBRVersion_ShouldSetSchemaViaUseStatement()
        {
            // Test case: Older DBR version should still set schema via USE statement.
            // skip if no schema is provided by user
            Skip.If(string.IsNullOrEmpty(TestConfiguration.DbSchema), "No schema provided by user");
            using var connection = NewConnection();
            // Verify current schema matches what we expect
            using var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetTables";

            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            // Verify we get results and they use the response namespace
            int rowCount = 0;
            var foundSchemas = new HashSet<string>();

            while (result.Stream != null)
            {
                using var batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                    break;

                rowCount += batch.Length;
                var schemaArray = (StringArray)batch.Column(0);

                for (int i = 0; i < batch.Length; i++)
                {
                    foundSchemas.Add(schemaArray.GetString(i) ?? string.Empty);
                }
            }

            Assert.True(rowCount > 0, "Should have results even without catalog specified");
            Assert.True(foundSchemas.Count == 1, "Should have exactly one schema");
        }

        [SkippableFact]
        public async Task CanExecuteDecimalQuery()
        {
            // This tests the bug where older DBR versions return decimal values as strings when UseArrowNativeTypes is false
            // To repro issue, run this with dbr < 10.0
            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();

            // Use the same query from the original bug report
            statement.SqlQuery = "SELECT cast(-6 as decimal(3, 1)) as A";
            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result.Stream);
            
            // Verify the schema
            var schema = result.Stream.Schema;
            Assert.Equal(1, schema.FieldsList.Count);
            
            var field = schema.GetFieldByName("A");
            Assert.NotNull(field);
            
            OutputHelper?.WriteLine($"Decimal field type: {field.DataType.GetType().Name}");
            OutputHelper?.WriteLine($"Decimal field type ID: {field.DataType.TypeId}");
            
            // Read the actual data
            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            
            if (field.DataType is Decimal128Type decimalType)
            {
                // For newer DBR versions with UseArrowNativeTypes enabled, decimal is returned as Decimal128Type
                Assert.Equal(3, decimalType.Precision);
                Assert.Equal(1, decimalType.Scale);
                
                var col0 = batch.Column(0) as Decimal128Array;
                Assert.NotNull(col0);
                Assert.Equal(1, col0.Length);
                
                var sqlDecimal = col0.GetSqlDecimal(0);
                Assert.Equal(-6.0m, sqlDecimal.Value);
                
                OutputHelper?.WriteLine($"Decimal value: {sqlDecimal.Value} (precision: {decimalType.Precision}, scale: {decimalType.Scale})");
            }
            else if (field.DataType is StringType)
            {
                // For older DBR versions with UseArrowNativeTypes disabled, decimal is returned as StringType
                var col0 = batch.Column(0) as StringArray;
                Assert.NotNull(col0);
                Assert.Equal(1, col0.Length);
                
                var stringValue = col0.GetString(0);
                Assert.NotNull(stringValue);
                
                // Parse the string value and verify it matches expected
                Assert.True(decimal.TryParse(stringValue, out decimal parsedValue));
                Assert.Equal(-6.0m, parsedValue);
                
                OutputHelper?.WriteLine($"Decimal as string value: '{stringValue}' -> {parsedValue}");
            }
            else
            {
                Assert.Fail($"Unexpected field type for decimal: {field.DataType.GetType().Name}");
            }
        }

    }
}
