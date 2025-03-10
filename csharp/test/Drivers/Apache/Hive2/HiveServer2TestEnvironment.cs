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
using System.Data.SqlTypes;
using System.Text;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2TestEnvironment : CommonTestEnvironment<ApacheTestConfiguration>
    {
        public class Factory : Factory<HiveServer2TestEnvironment>
        {
            public override HiveServer2TestEnvironment Create(Func<AdbcConnection> getConnection) => new(getConnection);
        }

        private HiveServer2TestEnvironment(Func<AdbcConnection> getConnection) : base(getConnection) { }

        public override string TestConfigVariable => "HIVE_TEST_CONFIG_FILE";

        public override string SqlDataResourceLocation => "Hive2/Resources/HiveData.sql";

        public override int ExpectedColumnCount => 17;

        public override AdbcDriver CreateNewDriver() => new HiveServer2Driver();

        public override string GetCreateTemporaryTableStatement(string tableName, string columns)
        {
            return string.Format("CREATE TABLE {0} ({1})", tableName, columns);
        }

        public override string Delimiter => "`";

        public override Dictionary<string, string> GetDriverParameters(ApacheTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrEmpty(testConfiguration.HostName))
            {
                parameters.Add(HiveServer2Parameters.HostName, testConfiguration.HostName!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Uri))
            {
                parameters.Add(AdbcOptions.Uri, testConfiguration.Uri!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Port))
            {
                parameters.Add(HiveServer2Parameters.Port, testConfiguration.Port!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Path))
            {
                parameters.Add(HiveServer2Parameters.Path, testConfiguration.Path!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Username))
            {
                parameters.Add(AdbcOptions.Username, testConfiguration.Username!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Password))
            {
                parameters.Add(AdbcOptions.Password, testConfiguration.Password!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.AuthType))
            {
                parameters.Add(HiveServer2Parameters.AuthType, testConfiguration.AuthType!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Type))
            {
                parameters.Add(HiveServer2Parameters.TransportType, testConfiguration.Type!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.DataTypeConversion))
            {
                parameters.Add(HiveServer2Parameters.DataTypeConv, testConfiguration.DataTypeConversion!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.BatchSize))
            {
                parameters.Add(ApacheParameters.BatchSize, testConfiguration.BatchSize!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.PollTimeMilliseconds))
            {
                parameters.Add(ApacheParameters.PollTimeMilliseconds, testConfiguration.PollTimeMilliseconds!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.ConnectTimeoutMilliseconds))
            {
                parameters.Add(HiveServer2Parameters.ConnectTimeoutMilliseconds, testConfiguration.ConnectTimeoutMilliseconds!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.QueryTimeoutSeconds))
            {
                parameters.Add(ApacheParameters.QueryTimeoutSeconds, testConfiguration.QueryTimeoutSeconds!);
            }
            return parameters;
        }

        public override string VendorVersion => ((HiveServer2Connection)Connection).VendorVersion;

        public override bool SupportsDelete => false;

        public override bool SupportsUpdate => false;

        public override bool SupportCatalogName => false;

        public override bool ValidateAffectedRows => false;

        public override string GetInsertStatement(string tableName, string columnName, string? value) =>
            string.Format("INSERT INTO {0} ({1}) SELECT {2}", tableName, columnName, value ?? "NULL");

        /// <summary>
        /// Get a <see cref="SampleDataBuilder"/> for data source specific
        /// data types.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// </remarks>
        public override SampleDataBuilder GetSampleDataBuilder()
        {
            SampleDataBuilder sampleDataBuilder = new();
            bool dataTypeIsFloat = DataTypeConversion.HasFlag(DataTypeConversion.Scalar);
            Type floatNetType = dataTypeIsFloat ? typeof(float) : typeof(double);
            Type floatArrowType = dataTypeIsFloat ? typeof(FloatType) : typeof(DoubleType);
            object floatValue;
            if (dataTypeIsFloat)
                floatValue = 1f;
            else
                floatValue = 1d;

            // standard values
            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "CAST(1 as BIGINT) as id, " +
                            "CAST(2 as INTEGER) as intcol, " +
                            "CAST(1 as FLOAT) as number_float, " +
                            "CAST(4.56 as DOUBLE) as number_double, " +
                            "CAST(4.56 as DECIMAL(3,2)) as decimalcol, " +
                            "CAST(9.9999999999999999999999999999999999999 as DECIMAL(38,37)) as big_decimal, " +
                            "CAST(True as BOOLEAN) as is_active, " +
                            "'John Doe' as name, " +
                            "CAST('abc123' as BINARY) as datacol, " +
                            "DATE '2023-09-08' as datecol, " +
                            "CAST('2023-09-08 12:34:56+00:00' as TIMESTAMP) as timestampcol, " +
                            "CAST('2023-09-08 12:34:56+00:00' as TIMESTAMP) + INTERVAL 20 YEARS as intervalcol, " +
                            "ARRAY(1, 2, 3) as numbers, " +
                            "named_struct('name', 'John Doe', 'age', 30) as person, " +
                            "map('name', CAST('Jane Doe' AS STRING), 'age', CAST(29 AS INT)) as personmap"
                            ,
                    ExpectedValues =
                    [
                        new("id", typeof(long), typeof(Int64Type), 1L),
                        new("intcol", typeof(int), typeof(Int32Type), 2),
                        new("number_float", typeof(float), typeof(FloatType), 1f),
                        new("number_double", typeof(double), typeof(DoubleType), 4.56),
                        new("decimalcol", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("4.56")),
                        new("big_decimal", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9.9999999999999999999999999999999999999")),
                        new("is_active", typeof(bool), typeof(BooleanType), true),
                        new("name", typeof(string), typeof(StringType), "John Doe"),
                        new("datacol", typeof(byte[]), typeof(BinaryType), UTF8Encoding.UTF8.GetBytes("abc123")),
                        new("datecol", typeof(DateTime), typeof(Date32Type), new DateTime(2023, 9, 8)),
                        new("timestampcol", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023, 9, 8, 12, 34, 56), TimeSpan.Zero)),
                        new("intervalcol", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2043, 9, 8, 12, 34, 56), TimeSpan.Zero)),
                        new("numbers", typeof(string), typeof(StringType), "[1,2,3]"),
                        new("person", typeof(string), typeof(StringType), """{"name":"John Doe","age":30}"""),
                        new("personmap", typeof(string), typeof(StringType), """{"name":"Jane Doe","age":"29"}""")
                     ]
                });

            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "CAST(NULL as BIGINT) as id, " +
                            "CAST(NULL as INTEGER) as intcol, " +
                            "CAST(NULL as FLOAT) as number_float, " +
                            "CAST(NULL as DOUBLE) as number_double, " +
                            "CAST(NULL as DECIMAL(3,2)) as decimalcol, " +
                            "CAST(NULL as DECIMAL(38,37)) as big_decimal, " +
                            "CAST(NULL as BOOLEAN) as is_active, " +
                            "CAST(NULL as STRING) as name, " +
                            "CAST(NULL as BINARY) as datacol, " +
                            "CAST(NULL as DATE) as datecol, " +
                            "CAST(NULL as TIMESTAMP) as timestampcol"
                            ,
                    ExpectedValues =
                    [
                        new("id", typeof(long), typeof(Int64Type), null),
                        new("intcol", typeof(int), typeof(Int32Type), null),
                        new("number_float", typeof(float), typeof(FloatType), null),
                        new("number_double", typeof(double), typeof(DoubleType), null),
                        new("decimalcol", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new("big_decimal", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new("is_active", typeof(bool), typeof(BooleanType), null),
                        new("name", typeof(string), typeof(StringType), null),
                        new("datacol", typeof(byte[]), typeof(BinaryType), null),
                        new("datecol", typeof(DateTime), typeof(Date32Type), null),
                        new("timestampcol", typeof(DateTimeOffset), typeof(TimestampType), null)
                     ]
                });

            return sampleDataBuilder;
        }
    }
}
