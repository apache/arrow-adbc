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
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// BigQuery-specific implementation of <see cref="AdbcConnection"/>
    /// </summary>
    public class BigQueryConnection : AdbcConnection
    {
        readonly IReadOnlyDictionary<string, string> properties;
        BigQueryClient? client;
        GoogleCredential? credential;

        const string infoDriverName = "ADBC BigQuery Driver";
        const string infoDriverVersion = "1.0.0";
        const string infoVendorName = "BigQuery";
        const string infoDriverArrowVersion = "1.0.0";

        readonly AdbcInfoCode[] infoSupportedCodes = new [] {
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName
        };

        public BigQueryConnection(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;

            // add the default value for now and set to true until C# has a BigDecimal
            Dictionary<string, string> modifiedProperties = this.properties.ToDictionary(k => k.Key, v => v.Value);
            modifiedProperties[BigQueryParameters.LargeDecimalsAsString] = BigQueryConstants.TreatLargeDecimalAsString;
            this.properties = new ReadOnlyDictionary<string, string>(modifiedProperties);
        }

        /// <summary>
        /// Initializes the internal BigQuery connection
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        internal BigQueryClient Open()
        {
            string? projectId = null;
            string? clientId = null;
            string? clientSecret = null;
            string? refreshToken = null;

            string tokenEndpoint = BigQueryConstants.TokenEndpoint;

            string? authenticationType = BigQueryConstants.UserAuthenticationType;

            // TODO: handle token expiration

            if (!this.properties.TryGetValue(BigQueryParameters.ProjectId, out projectId))
                throw new ArgumentException($"The {BigQueryParameters.ProjectId} parameter is not present");

            if (this.properties.TryGetValue(BigQueryParameters.AuthenticationType, out string? newAuthenticationType))
            {
                if (!string.IsNullOrEmpty(newAuthenticationType))
                    authenticationType = newAuthenticationType;

                if (!authenticationType.Equals(BigQueryConstants.UserAuthenticationType, StringComparison.OrdinalIgnoreCase) &&
                    !authenticationType.Equals(BigQueryConstants.ServiceAccountAuthenticationType, StringComparison.OrdinalIgnoreCase))
                {
                    throw new ArgumentException($"The {BigQueryParameters.AuthenticationType} parameter can only be `{BigQueryConstants.UserAuthenticationType}` or `{BigQueryConstants.ServiceAccountAuthenticationType}`");
                }
            }

            if (!string.IsNullOrEmpty(authenticationType) && authenticationType.Equals(BigQueryConstants.UserAuthenticationType, StringComparison.OrdinalIgnoreCase))
            {
                if (!this.properties.TryGetValue(BigQueryParameters.ClientId, out clientId))
                    throw new ArgumentException($"The {BigQueryParameters.ClientId} parameter is not present");

                if (!this.properties.TryGetValue(BigQueryParameters.ClientSecret, out clientSecret))
                    throw new ArgumentException($"The {BigQueryParameters.ClientSecret} parameter is not present");

                if (!this.properties.TryGetValue(BigQueryParameters.RefreshToken, out refreshToken))
                    throw new ArgumentException($"The {BigQueryParameters.RefreshToken} parameter is not present");

                this.credential = ApplyScopes(GoogleCredential.FromAccessToken(GetAccessToken(clientId, clientSecret, refreshToken, tokenEndpoint)));
            }
            else
            {
                string? json = string.Empty;

                if (!this.properties.TryGetValue(BigQueryParameters.JsonCredential, out json))
                    throw new ArgumentException($"The {BigQueryParameters.JsonCredential} parameter is not present");

                this.credential = ApplyScopes(GoogleCredential.FromJson(json));
            }

            BigQueryClient client = BigQueryClient.Create(projectId, this.credential);
            this.client = client;
            return client;
        }

        /// <summary>
        /// Apply any additional scopes to the credential.
        /// </summary>
        /// <param name="credential"><see cref="GoogleCredential"/></param>
        /// <returns></returns>
        private GoogleCredential ApplyScopes(GoogleCredential credential)
        {
            if (credential == null) throw new ArgumentNullException(nameof(credential));

            if (this.properties.TryGetValue(BigQueryParameters.Scopes, out string? scopes))
            {
                if (!string.IsNullOrEmpty(scopes))
                {
                    IEnumerable<string> parsedScopes = scopes.Split(',').Where(x => x.Length > 0);

                    return credential.CreateScoped(parsedScopes);
                }
            }

            return credential;
        }

        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            const int strValTypeID = 0;

            UnionType infoUnionType = new UnionType(
                new Field[]
                {
                    new Field("string_value", StringType.Default, true),
                    new Field("bool_value", BooleanType.Default, true),
                    new Field("int64_value", Int64Type.Default, true),
                    new Field("int32_bitmask", Int32Type.Default, true),
                    new Field(
                        "string_list",
                        new ListType(
                            new Field("item", StringType.Default, true)
                        ),
                        false
                    ),
                    new Field(
                        "int32_to_int32_list_map",
                        new ListType(
                            new Field("entries", new StructType(
                                new Field[]
                                {
                                    new Field("key", Int32Type.Default, false),
                                    new Field("value", Int32Type.Default, true),
                                }
                                ), false)
                        ),
                        true
                    )
                },
                new int[] { 0, 1, 2, 3, 4, 5 }.ToArray(),
                UnionMode.Dense);

            if (codes.Count == 0)
            {
                codes = infoSupportedCodes;
            }

            UInt32Array.Builder infoNameBuilder = new UInt32Array.Builder();
            ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>();
            ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>();
            StringArray.Builder stringInfoBuilder = new StringArray.Builder();
            int nullCount = 0;
            int arrayLength = codes.Count;

            foreach (AdbcInfoCode code in codes)
            {
                switch (code)
                {
                    case AdbcInfoCode.DriverName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoDriverName);
                        break;
                    case AdbcInfoCode.DriverVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoDriverVersion);
                        break;
                    case AdbcInfoCode.DriverArrowVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoDriverArrowVersion);
                        break;
                    case AdbcInfoCode.VendorName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoVendorName);
                        break;
                    default:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.AppendNull();
                        nullCount++;
                        break;
                }
            }

            StructType entryType = new StructType(
                new Field[] {
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            StructArray entriesDataArray = new StructArray(entryType, 0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            IArrowArray[] childrenArrays = new IArrowArray[]
            {
                stringInfoBuilder.Build(),
                new BooleanArray.Builder().Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                new List<IArrowArray?>(){ entriesDataArray }.BuildListArrayForType(entryType)
            };

            DenseUnionArray infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                infoNameBuilder.Build(),
                infoValue
            };
            StandardSchemas.GetInfoSchema.Validate(dataArrays);

            return new BigQueryInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays);
        }

        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            IArrowArray[] dataArrays = GetCatalogs(depth, catalogPattern, dbSchemaPattern,
                tableNamePattern, tableTypes, columnNamePattern);

            return new BigQueryInfoArrowStream(StandardSchemas.GetObjectsSchema, dataArrays);
        }

        /// <summary>
        /// Executes the query using the BigQueryClient.
        /// </summary>
        /// <param name="sql">The query to execute.</param>
        /// <param name="parameters">Parameters to include.</param>
        /// <param name="queryOptions">Additional query options.</param>
        /// <param name="resultsOptions">Additional result options.</param>
        /// <returns></returns>
        /// <remarks>
        /// Can later add logging or metrics around query calls.
        /// </remarks>
        private BigQueryResults? ExecuteQuery(string sql, IEnumerable<BigQueryParameter>? parameters, QueryOptions? queryOptions = null, GetQueryResultsOptions? resultsOptions = null)
        {
            BigQueryResults? result = this.client?.ExecuteQuery(sql, parameters, queryOptions, resultsOptions);
            return result;
        }

        private IArrowArray[] GetCatalogs(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            StringArray.Builder catalogNameBuilder = new StringArray.Builder();
            List<IArrowArray?> catalogDbSchemasValues = new List<IArrowArray?>();
            string catalogRegexp = PatternToRegEx(catalogPattern);
            PagedEnumerable<ProjectList, CloudProject>? catalogs = this.client?.ListProjects();

            if (catalogs != null)
            {
                foreach (CloudProject catalog in catalogs)
                {
                    if (Regex.IsMatch(catalog.ProjectId, catalogRegexp, RegexOptions.IgnoreCase))
                    {
                        catalogNameBuilder.Append(catalog.ProjectId);

                        if (depth == GetObjectsDepth.Catalogs)
                        {
                            catalogDbSchemasValues.Add(null);
                        }
                        else
                        {
                            catalogDbSchemasValues.Add(GetDbSchemas(
                                depth, catalog.ProjectId, dbSchemaPattern,
                                tableNamePattern, tableTypes, columnNamePattern));
                        }
                    }
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                catalogNameBuilder.Build(),
                catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema)),
            };
            StandardSchemas.GetObjectsSchema.Validate(dataArrays);

            return dataArrays;
        }

        private StructArray GetDbSchemas(
            GetObjectsDepth depth,
            string catalog,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            StringArray.Builder dbSchemaNameBuilder = new StringArray.Builder();
            List<IArrowArray?> dbSchemaTablesValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string dbSchemaRegexp = PatternToRegEx(dbSchemaPattern);

            PagedEnumerable<DatasetList, BigQueryDataset>? schemas = this.client?.ListDatasets(catalog);

            if (schemas != null)
            {
                foreach (BigQueryDataset schema in schemas)
                {
                    if (Regex.IsMatch(schema.Reference.DatasetId, dbSchemaRegexp, RegexOptions.IgnoreCase))
                    {
                        dbSchemaNameBuilder.Append(schema.Reference.DatasetId);
                        length++;
                        nullBitmapBuffer.Append(true);

                        if (depth == GetObjectsDepth.DbSchemas)
                        {
                            dbSchemaTablesValues.Add(null);
                        }
                        else
                        {
                            dbSchemaTablesValues.Add(GetTableSchemas(
                                depth, catalog, schema.Reference.DatasetId,
                                tableNamePattern, tableTypes, columnNamePattern));
                        }
                    }
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                dbSchemaNameBuilder.Build(),
                dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema)),
            };
            StandardSchemas.DbSchemaSchema.Validate(dataArrays);

            return new StructArray(
                new StructType(StandardSchemas.DbSchemaSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetTableSchemas(
            GetObjectsDepth depth,
            string catalog,
            string dbSchema,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            StringArray.Builder tableNameBuilder = new StringArray.Builder();
            StringArray.Builder tableTypeBuilder = new StringArray.Builder();
            List<IArrowArray?> tableColumnsValues = new List<IArrowArray?>();
            List<IArrowArray?> tableConstraintsValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = string.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.TABLES",
                Sanitize(catalog), Sanitize(dbSchema));

            if (tableNamePattern != null)
            {
                query = string.Concat(query, string.Format(" WHERE table_name LIKE '{0}'", Sanitize(tableNamePattern)));
                if (tableTypes?.Count > 0)
                {
                    IEnumerable<string> sanitizedTypes = tableTypes.Select(x => Sanitize(x));
                    query = string.Concat(query, string.Format(" AND table_type IN ('{0}')", string.Join("', '", sanitizedTypes).ToUpper()));
                }
            }
            else
            {
                if (tableTypes?.Count > 0)
                {
                    IEnumerable<string> sanitizedTypes = tableTypes.Select(x => Sanitize(x));
                    query = string.Concat(query, string.Format(" WHERE table_type IN ('{0}')", string.Join("', '", sanitizedTypes).ToUpper()));
                }
            }

            BigQueryResults? result = ExecuteQuery(query, parameters: null);

            if (result != null)
            {
                bool includeConstraints = true;

                if (this.properties.TryGetValue(BigQueryParameters.IncludeConstraintsWithGetObjects, out string? includeConstraintsValue))
                {
                    bool.TryParse(includeConstraintsValue, out includeConstraints);
                }

                foreach (BigQueryRow row in result)
                {
                    tableNameBuilder.Append(GetValue(row["table_name"]));
                    tableTypeBuilder.Append(GetValue(row["table_type"]));
                    nullBitmapBuffer.Append(true);
                    length++;

                    if (includeConstraints)
                    {
                        tableConstraintsValues.Add(GetConstraintSchema(
                            depth, catalog, dbSchema, GetValue(row["table_name"]), columnNamePattern));
                    }
                    else
                    {
                        tableConstraintsValues.Add(null);
                    }

                    if (depth == GetObjectsDepth.Tables)
                    {
                        tableColumnsValues.Add(null);
                    }
                    else
                    {
                        tableColumnsValues.Add(GetColumnSchema(catalog, dbSchema, GetValue(row["table_name"]), columnNamePattern));
                    }
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                tableNameBuilder.Build(),
                tableTypeBuilder.Build(),
                tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
                tableConstraintsValues.BuildListArrayForType(new StructType(StandardSchemas.ConstraintSchema))
            };
            StandardSchemas.TableSchema.Validate(dataArrays);

            return new StructArray(
                new StructType(StandardSchemas.TableSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetColumnSchema(
            string catalog,
            string dbSchema,
            string table,
            string? columnNamePattern)
        {
            StringArray.Builder columnNameBuilder = new StringArray.Builder();
            Int32Array.Builder ordinalPositionBuilder = new Int32Array.Builder();
            StringArray.Builder remarksBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcDataTypeBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcTypeNameBuilder = new StringArray.Builder();
            Int32Array.Builder xdbcColumnSizeBuilder = new Int32Array.Builder();
            Int16Array.Builder xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNullableBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcColumnDefBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcDatetimeSubBuilder = new Int16Array.Builder();
            Int32Array.Builder xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            StringArray.Builder xdbcIsNullableBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeCatalogBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeSchemaBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeTableBuilder = new StringArray.Builder();
            BooleanArray.Builder xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            BooleanArray.Builder xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = string.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{2}'",
                Sanitize(catalog), Sanitize(dbSchema), Sanitize(table));

            if (columnNamePattern != null)
            {
                query = string.Concat(query, string.Format("AND column_name LIKE '{0}'", Sanitize(columnNamePattern)));
            }

            BigQueryResults? result = ExecuteQuery(query, parameters: null);

            if (result != null)
            {
                foreach (BigQueryRow row in result)
                {
                    columnNameBuilder.Append(GetValue(row["column_name"]));
                    ordinalPositionBuilder.Append((int)(long)row["ordinal_position"]);
                    remarksBuilder.Append("");

                    string dataType = ToTypeName(GetValue(row["data_type"]));

                    if (dataType.StartsWith("NUMERIC") || dataType.StartsWith("DECIMAL") || dataType.StartsWith("BIGNUMERIC") || dataType.StartsWith("BIGDECIMAL"))
                    {
                        ParsedDecimalValues values = ParsePrecisionAndScale(dataType);
                        xdbcColumnSizeBuilder.Append(values.Precision);
                        xdbcDecimalDigitsBuilder.Append(Convert.ToInt16(values.Scale));
                    }
                    else
                    {
                        xdbcColumnSizeBuilder.AppendNull();
                        xdbcDecimalDigitsBuilder.AppendNull();
                    }

                    xdbcDataTypeBuilder.AppendNull();
                    xdbcTypeNameBuilder.Append(dataType);
                    xdbcNumPrecRadixBuilder.AppendNull();
                    xdbcNullableBuilder.AppendNull();
                    xdbcColumnDefBuilder.AppendNull();
                    xdbcSqlDataTypeBuilder.Append((short)ToXdbcDataType(dataType));
                    xdbcDatetimeSubBuilder.AppendNull();
                    xdbcCharOctetLengthBuilder.AppendNull();
                    xdbcIsNullableBuilder.Append(row["is_nullable"].ToString());
                    xdbcScopeCatalogBuilder.AppendNull();
                    xdbcScopeSchemaBuilder.AppendNull();
                    xdbcScopeTableBuilder.AppendNull();
                    xdbcIsAutoincrementBuilder.AppendNull();
                    xdbcIsGeneratedcolumnBuilder.Append(GetValue(row["is_generated"]).ToUpper() == "YES");
                    nullBitmapBuffer.Append(true);
                    length++;
                }
            }
            IArrowArray[] dataArrays = new IArrowArray[]
            {
                columnNameBuilder.Build(),
                ordinalPositionBuilder.Build(),
                remarksBuilder.Build(),
                xdbcDataTypeBuilder.Build(),
                xdbcTypeNameBuilder.Build(),
                xdbcColumnSizeBuilder.Build(),
                xdbcDecimalDigitsBuilder.Build(),
                xdbcNumPrecRadixBuilder.Build(),
                xdbcNullableBuilder.Build(),
                xdbcColumnDefBuilder.Build(),
                xdbcSqlDataTypeBuilder.Build(),
                xdbcDatetimeSubBuilder.Build(),
                xdbcCharOctetLengthBuilder.Build(),
                xdbcIsNullableBuilder.Build(),
                xdbcScopeCatalogBuilder.Build(),
                xdbcScopeSchemaBuilder.Build(),
                xdbcScopeTableBuilder.Build(),
                xdbcIsAutoincrementBuilder.Build(),
                xdbcIsGeneratedcolumnBuilder.Build()
            };
            StandardSchemas.ColumnSchema.Validate(dataArrays);

            return new StructArray(
                new StructType(StandardSchemas.ColumnSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetConstraintSchema(
            GetObjectsDepth depth,
            string catalog,
            string dbSchema,
            string table,
            string? columnNamePattern)
        {
            StringArray.Builder constraintNameBuilder = new StringArray.Builder();
            StringArray.Builder constraintTypeBuilder = new StringArray.Builder();
            List<IArrowArray?> constraintColumnNamesValues = new List<IArrowArray?>();
            List<IArrowArray?> constraintColumnUsageValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = string.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_name = '{2}'",
               Sanitize(catalog), Sanitize(dbSchema), Sanitize(table));

            BigQueryResults? result = ExecuteQuery(query, parameters: null);

            if (result != null)
            {
                foreach (BigQueryRow row in result)
                {
                    string constraintName = GetValue(row["constraint_name"]);
                    constraintNameBuilder.Append(constraintName);
                    string constraintType = GetValue(row["constraint_type"]);
                    constraintTypeBuilder.Append(constraintType);
                    nullBitmapBuffer.Append(true);
                    length++;

                    if (depth == GetObjectsDepth.All || depth == GetObjectsDepth.Tables)
                    {
                        constraintColumnNamesValues.Add(GetConstraintColumnNames(
                            catalog, dbSchema, table, constraintName));
                        if (constraintType.ToUpper() == "FOREIGN KEY")
                        {
                            constraintColumnUsageValues.Add(GetConstraintsUsage(
                                catalog, dbSchema, table, constraintName));
                        }
                        else
                        {
                            constraintColumnUsageValues.Add(null);
                        }
                    }
                    else
                    {
                        constraintColumnNamesValues.Add(null);
                        constraintColumnUsageValues.Add(null);
                    }
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                constraintNameBuilder.Build(),
                constraintTypeBuilder.Build(),
                constraintColumnNamesValues.BuildListArrayForType(StringType.Default),
                constraintColumnUsageValues.BuildListArrayForType(new StructType(StandardSchemas.UsageSchema))
            };
            StandardSchemas.ConstraintSchema.Validate(dataArrays);

            return new StructArray(
                new StructType(StandardSchemas.ConstraintSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StringArray GetConstraintColumnNames(
            string catalog,
            string dbSchema,
            string table,
            string constraintName)
        {
            string query = string.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE table_name = '{2}' AND constraint_name = '{3}' ORDER BY ordinal_position",
               Sanitize(catalog), Sanitize(dbSchema), Sanitize(table), Sanitize(constraintName));

            StringArray.Builder constraintColumnNamesBuilder = new StringArray.Builder();

            BigQueryResults? result = ExecuteQuery(query, parameters: null);

            if (result != null)
            {
                foreach (BigQueryRow row in result)
                {
                    string column = GetValue(row["column_name"]);
                    constraintColumnNamesBuilder.Append(column);
                }
            }

            return constraintColumnNamesBuilder.Build();
        }

        private StructArray GetConstraintsUsage(
            string catalog,
            string dbSchema,
            string table,
            string constraintName)
        {
            StringArray.Builder constraintFkCatalogBuilder = new StringArray.Builder();
            StringArray.Builder constraintFkDbSchemaBuilder = new StringArray.Builder();
            StringArray.Builder constraintFkTableBuilder = new StringArray.Builder();
            StringArray.Builder constraintFkColumnNameBuilder = new StringArray.Builder();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = string.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE constraint_name = '{2}'",
               Sanitize(catalog), Sanitize(dbSchema), Sanitize(constraintName));

            BigQueryResults? result = ExecuteQuery(query, parameters: null);

            if (result != null)
            {
                foreach (BigQueryRow row in result)
                {
                    string constraint_catalog = GetValue(row["constraint_catalog"]);
                    string constraint_schema = GetValue(row["constraint_schema"]);
                    string table_name = GetValue(row["table_name"]);
                    string column_name = GetValue(row["column_name"]);

                    constraintFkCatalogBuilder.Append(constraint_catalog);
                    constraintFkDbSchemaBuilder.Append(constraint_schema);
                    constraintFkTableBuilder.Append(table_name);
                    constraintFkColumnNameBuilder.Append(column_name);

                    nullBitmapBuffer.Append(true);
                    length++;
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                constraintFkCatalogBuilder.Build(),
                constraintFkDbSchemaBuilder.Build(),
                constraintFkTableBuilder.Build(),
                constraintFkColumnNameBuilder.Build()
            };
            StandardSchemas.UsageSchema.Validate(dataArrays);

            return new StructArray(
                new StructType(StandardSchemas.UsageSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private string PatternToRegEx(string? pattern)
        {
            if (pattern == null)
                return ".*";

            StringBuilder builder = new StringBuilder("(?i)^");
            string convertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(convertedPattern);
            builder.Append("$");

            return builder.ToString();
        }

        private string ToTypeName(string type)
        {
            int index = Math.Min(type.IndexOf("("), type.IndexOf("<"));
            string dataType = index == -1 ? type : type.Substring(0, index);
            return dataType;
        }

        private XdbcDataType ToXdbcDataType(string type)
        {
            switch (type)
            {
                case "INTEGER" or "INT64":
                    return XdbcDataType.XdbcDataType_XDBC_INTEGER;
                case "FLOAT" or "FLOAT64":
                    return XdbcDataType.XdbcDataType_XDBC_FLOAT;
                case "BOOL" or "BOOLEAN":
                    return XdbcDataType.XdbcDataType_XDBC_BIT;
                case "STRING" or "GEOGRAPHY" or "JSON":
                    return XdbcDataType.XdbcDataType_XDBC_VARCHAR;
                case "BYTES":
                    return XdbcDataType.XdbcDataType_XDBC_BINARY;
                case "DATETIME":
                    return XdbcDataType.XdbcDataType_XDBC_DATETIME;
                case "TIMESTAMP":
                    return XdbcDataType.XdbcDataType_XDBC_TIMESTAMP;
                case "TIME":
                    return XdbcDataType.XdbcDataType_XDBC_TIME;
                case "DATE":
                    return XdbcDataType.XdbcDataType_XDBC_DATE;
                case "RECORD" or "STRUCT":
                    return XdbcDataType.XdbcDataType_XDBC_VARBINARY;
                case "NUMERIC" or "DECIMAL" or "BIGNUMERIC" or "BIGDECIMAL":
                    return XdbcDataType.XdbcDataType_XDBC_NUMERIC;
                default:

                    // in SqlDecimal, an OverflowException is thrown for decimals with scale > 28
                    // so the XDBC type needs to map the SqlDecimal type
                    int decimalMaxScale = 28;

                    if (type.StartsWith("NUMERIC("))
                    {
                        ParsedDecimalValues parsedDecimalValues = ParsePrecisionAndScale(type);

                        if (parsedDecimalValues.Scale <= decimalMaxScale)
                            return XdbcDataType.XdbcDataType_XDBC_DECIMAL;
                        else
                            return XdbcDataType.XdbcDataType_XDBC_VARCHAR;
                    }

                    if (type.StartsWith("BIGNUMERIC("))
                    {
                        if (bool.Parse(this.properties[BigQueryParameters.LargeDecimalsAsString]))
                        {
                            return XdbcDataType.XdbcDataType_XDBC_VARCHAR;
                        }
                        else
                        {
                            ParsedDecimalValues parsedDecimalValues = ParsePrecisionAndScale(type);

                            if (parsedDecimalValues.Scale <= decimalMaxScale)
                                return XdbcDataType.XdbcDataType_XDBC_DECIMAL;
                            else
                                return XdbcDataType.XdbcDataType_XDBC_VARCHAR;
                        }
                    }

                    if (type.StartsWith("STRUCT"))
                        return XdbcDataType.XdbcDataType_XDBC_VARCHAR;

                    return XdbcDataType.XdbcDataType_XDBC_UNKNOWN_TYPE;
            }
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            string query = string.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{2}'",
                Sanitize(catalog), Sanitize(dbSchema), Sanitize(tableName));

            BigQueryResults? result = ExecuteQuery(query, parameters: null);

            List<Field> fields = new List<Field>();

            if (result != null)
            {
                foreach (BigQueryRow row in result)
                {
                    fields.Add(DescToField(row));
                }
            }

            return new Schema(fields, null);
        }

        private Field DescToField(BigQueryRow row)
        {
            Dictionary<string, string> metaData = new Dictionary<string, string>();
            metaData.Add("PRIMARY_KEY", "");
            metaData.Add("ORDINAL_POSITION", GetValue(row["ordinal_position"]));
            metaData.Add("DATA_TYPE", GetValue(row["data_type"]));

            Field.Builder fieldBuilder = SchemaFieldGenerator(GetValue(row["column_name"]).ToLower(), GetValue(row["data_type"]));
            fieldBuilder.Metadata(metaData);

            if (!GetValue(row["is_nullable"]).Equals("YES", StringComparison.OrdinalIgnoreCase))
            {
                fieldBuilder.Nullable(false);
            }

            fieldBuilder.Name(GetValue(row["column_name"]).ToLower());

            return fieldBuilder.Build();
        }

        private string GetValue(object value)
        {
            switch (value)
            {
                case string sValue:
                    return sValue;
                default:
                    if (value != null)
                    {
                        string? sValue = value.ToString();
                        return sValue ?? string.Empty;
                    }
                    throw new InvalidOperationException($"Cannot parse {value}");
            }
        }

        private Field.Builder SchemaFieldGenerator(string name, string type)
        {
            int index = type.IndexOf("(");
            index = index == -1 ? type.IndexOf("<") : Math.Max(index, type.IndexOf("<"));
            string dataType = index == -1 ? type : type.Substring(0, index);

            return GetFieldBuilder(name, type, dataType, index);
        }

        private Field.Builder GetFieldBuilder(string name, string type, string dataType, int index)
        {
            Field.Builder fieldBuilder = new Field.Builder();
            fieldBuilder.Name(name);

            switch (dataType)
            {
                case "INTEGER" or "INT64":
                    return fieldBuilder.DataType(Int64Type.Default);
                case "FLOAT" or "FLOAT64":
                    return fieldBuilder.DataType(DoubleType.Default);
                case "BOOL" or "BOOLEAN":
                    return fieldBuilder.DataType(BooleanType.Default);
                case "STRING" or "GEOGRAPHY" or "JSON":
                    return fieldBuilder.DataType(StringType.Default);
                case "BYTES":
                    return fieldBuilder.DataType(BinaryType.Default);
                case "DATETIME":
                    return fieldBuilder.DataType(TimestampType.Default);
                case "TIMESTAMP":
                    return fieldBuilder.DataType(TimestampType.Default);
                case "TIME":
                    return fieldBuilder.DataType(Time64Type.Default);
                case "DATE":
                    return fieldBuilder.DataType(Date64Type.Default);
                case "RECORD" or "STRUCT":
                    string fieldRecords = type.Substring(index + 1);
                    fieldRecords = fieldRecords.Remove(fieldRecords.Length - 1);
                    List<Field> nestedFields = new List<Field>();

                    foreach (string record in fieldRecords.Split(','))
                    {
                        string fieldRecord = record.Trim();
                        string fieldName = fieldRecord.Split(' ')[0];
                        string fieldType = fieldRecord.Split(' ')[1];
                        nestedFields.Add(SchemaFieldGenerator(fieldName, fieldType).Build());
                    }

                    return fieldBuilder.DataType(new StructType(nestedFields));
                case "NUMERIC" or "DECIMAL":
                    ParsedDecimalValues values128 = ParsePrecisionAndScale(type);
                    return fieldBuilder.DataType(new Decimal128Type(values128.Precision, values128.Scale));
                case "BIGNUMERIC" or "BIGDECIMAL":
                    ParsedDecimalValues values256 = ParsePrecisionAndScale(type);
                    return fieldBuilder.DataType(new Decimal256Type(values256.Precision, values256.Scale));
                case "ARRAY":
                    string arrayType = type.Substring(dataType.Length).Replace("<", "").Replace(">", "");
                    return GetFieldBuilder(name, type, arrayType, index);

                default: throw new InvalidOperationException($"{dataType} cannot be handled");
            }
        }

        private class ParsedDecimalValues
        {
            public int Precision { get; set; }
            public int Scale { get; set; }
        }

        private ParsedDecimalValues ParsePrecisionAndScale(string type)
        {
            if (string.IsNullOrWhiteSpace(type)) throw new ArgumentNullException(nameof(type));

            string[] values = type.Substring(type.IndexOf("(") + 1).TrimEnd(')').Split(",".ToCharArray());

            return new ParsedDecimalValues()
            {
                Precision = Convert.ToInt32(values[0]),
                Scale = Convert.ToInt32(values[1])
            };
        }

        public override IArrowArrayStream GetTableTypes()
        {
            StringArray.Builder tableTypesBuilder = new StringArray.Builder();
            tableTypesBuilder.AppendRange(new string[] { "BASE TABLE", "VIEW" });

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                tableTypesBuilder.Build()
            };
            StandardSchemas.TableTypesSchema.Validate(dataArrays);

            return new BigQueryInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
        }

        public override AdbcStatement CreateStatement()
        {
            if (this.credential == null)
            {
                throw new InvalidOperationException();
            }

            if (this.client == null)
            {
                this.client = Open();
            }

            BigQueryStatement statement = new BigQueryStatement(this.client, this.credential);
            statement.Options = ParseOptions();
            return statement;
        }

        private IReadOnlyDictionary<string, string> ParseOptions()
        {
            Dictionary<string, string> options = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string> keyValuePair in this.properties)
            {
                if (keyValuePair.Key == BigQueryParameters.AllowLargeResults)
                {
                    options[keyValuePair.Key] = keyValuePair.Value;
                }
                if (keyValuePair.Key == BigQueryParameters.UseLegacySQL)
                {
                    options[keyValuePair.Key] = keyValuePair.Value;
                }
                if (keyValuePair.Key == BigQueryParameters.LargeDecimalsAsString)
                {
                    options[keyValuePair.Key] = keyValuePair.Value;
                }
                if (keyValuePair.Key == BigQueryParameters.LargeResultsDestinationTable)
                {
                    options[keyValuePair.Key] = keyValuePair.Value;
                }
            }

            return new ReadOnlyDictionary<string, string>(options);
        }

        public override void Dispose()
        {
            this.client?.Dispose();
            this.client = null;
        }

        private static Regex sanitizedInputRegex = new Regex("^[a-zA-Z0-9_-]+");

        private string Sanitize(string? input)
        {
            if (string.IsNullOrEmpty(input))
                return string.Empty;

            bool isValidInput = sanitizedInputRegex.IsMatch(input);

            if (isValidInput)
            {
                return input!;
            }
            else
            {
                throw new AdbcException($"{input} is invalid", AdbcStatusCode.InvalidArgument);
            }
        }

        /// <summary>
        /// Gets the access token from the token endpoint.
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="clientSecret"></param>
        /// <param name="refreshToken"></param>
        /// <param name="tokenEndpoint"></param>
        /// <returns></returns>
        private string? GetAccessToken(string clientId, string clientSecret, string refreshToken, string tokenEndpoint)
        {
            string body = string.Format(
                "grant_type=refresh_token&client_id={0}&client_secret={1}&refresh_token={2}",
                clientId,
                clientSecret,
                Uri.EscapeDataString(refreshToken));

            HttpClient httpClient = new HttpClient();

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
            request.Headers.Add("Accept", "application/json");
            request.Content = new StringContent(body, Encoding.UTF8, "application/x-www-form-urlencoded");
            HttpResponseMessage response = httpClient.SendAsync(request).Result;
            string responseBody = response.Content.ReadAsStringAsync().Result;

            BigQueryTokenResponse? bigQueryTokenResponse = JsonSerializer.Deserialize<BigQueryTokenResponse>(responseBody);

            return bigQueryTokenResponse?.AccessToken;
        }

        enum XdbcDataType
        {
            XdbcDataType_XDBC_UNKNOWN_TYPE = 0,
            XdbcDataType_XDBC_CHAR = 1,
            XdbcDataType_XDBC_NUMERIC = 2,
            XdbcDataType_XDBC_DECIMAL = 3,
            XdbcDataType_XDBC_INTEGER = 4,
            XdbcDataType_XDBC_SMALLINT = 5,
            XdbcDataType_XDBC_FLOAT = 6,
            XdbcDataType_XDBC_REAL = 7,
            XdbcDataType_XDBC_DOUBLE = 8,
            XdbcDataType_XDBC_DATETIME = 9,
            XdbcDataType_XDBC_INTERVAL = 10,
            XdbcDataType_XDBC_VARCHAR = 12,
            XdbcDataType_XDBC_DATE = 91,
            XdbcDataType_XDBC_TIME = 92,
            XdbcDataType_XDBC_TIMESTAMP = 93,
            XdbcDataType_XDBC_LONGVARCHAR = -1,
            XdbcDataType_XDBC_BINARY = -2,
            XdbcDataType_XDBC_VARBINARY = -3,
            XdbcDataType_XDBC_LONGVARBINARY = -4,
            XdbcDataType_XDBC_BIGINT = -5,
            XdbcDataType_XDBC_TINYINT = -6,
            XdbcDataType_XDBC_BIT = -7,
            XdbcDataType_XDBC_WCHAR = -8,
            XdbcDataType_XDBC_WVARCHAR = -9,
        }
    }
}
