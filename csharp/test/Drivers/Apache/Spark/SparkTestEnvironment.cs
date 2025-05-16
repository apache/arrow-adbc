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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class SparkTestEnvironment : CommonTestEnvironment<SparkTestConfiguration>
    {
        public class Factory : Factory<SparkTestEnvironment>
        {
            public override SparkTestEnvironment Create(Func<AdbcConnection> getConnection) => new(getConnection);
        }

        private SparkTestEnvironment(Func<AdbcConnection> getConnection) : base(getConnection) { }

        public override string TestConfigVariable => "SPARK_TEST_CONFIG_FILE";

        public override string SqlDataResourceLocation => "Spark/Resources/SparkData.sql";

        public override int ExpectedColumnCount => 17;

        public override AdbcDriver CreateNewDriver() => new SparkDriver();

        public override string GetCreateTemporaryTableStatement(string tableName, string columns)
        {
            return string.Format("CREATE TABLE {0} ({1})", tableName, columns);
        }

        public override string Delimiter => "`";

        public override Dictionary<string, string> GetDriverParameters(SparkTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrEmpty(testConfiguration.HostName))
            {
                parameters.Add(SparkParameters.HostName, testConfiguration.HostName!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Uri))
            {
                parameters.Add(AdbcOptions.Uri, testConfiguration.Uri!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Port))
            {
                parameters.Add(SparkParameters.Port, testConfiguration.Port!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Path))
            {
                parameters.Add(SparkParameters.Path, testConfiguration.Path!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Token))
            {
                parameters.Add(SparkParameters.Token, testConfiguration.Token!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.AccessToken))
            {
                parameters.Add(SparkParameters.AccessToken, testConfiguration.AccessToken);
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
                parameters.Add(SparkParameters.AuthType, testConfiguration.AuthType!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Type))
            {
                parameters.Add(SparkParameters.Type, testConfiguration.Type!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.DataTypeConversion))
            {
                parameters.Add(SparkParameters.DataTypeConv, testConfiguration.DataTypeConversion!);
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
                parameters.Add(SparkParameters.ConnectTimeoutMilliseconds, testConfiguration.ConnectTimeoutMilliseconds!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.QueryTimeoutSeconds))
            {
                parameters.Add(ApacheParameters.QueryTimeoutSeconds, testConfiguration.QueryTimeoutSeconds!);
            }
            if (testConfiguration.HttpOptions != null)
            {
                if (testConfiguration.HttpOptions.Tls != null)
                {
                    TlsTestConfiguration tlsOptions = testConfiguration.HttpOptions.Tls;
                    if (tlsOptions.Enabled.HasValue)
                    {
                        parameters.Add(HttpTlsOptions.IsTlsEnabled, tlsOptions.Enabled.Value.ToString());
                    }
                    if (tlsOptions.AllowSelfSigned.HasValue)
                    {
                        parameters.Add(HttpTlsOptions.AllowSelfSigned, tlsOptions.AllowSelfSigned.Value.ToString());
                    }
                    if (tlsOptions.AllowHostnameMismatch.HasValue)
                    {
                        parameters.Add(HttpTlsOptions.AllowHostnameMismatch, tlsOptions.AllowHostnameMismatch.Value.ToString());
                    }
                    if (tlsOptions.DisableServerCertificateValidation.HasValue)
                    {
                        parameters.Add(HttpTlsOptions.DisableServerCertificateValidation, tlsOptions.DisableServerCertificateValidation.Value.ToString());
                    }
                    if (!string.IsNullOrEmpty(tlsOptions.TrustedCertificatePath))
                    {
                        parameters.Add(HttpTlsOptions.TrustedCertificatePath, tlsOptions.TrustedCertificatePath!);
                    }
                }

                // Add proxy configuration if provided
                if (testConfiguration.HttpOptions.Proxy != null)
                {
                    ProxyTestConfiguration proxyOptions = testConfiguration.HttpOptions.Proxy;
                    if (!string.IsNullOrEmpty(proxyOptions.UseProxy))
                    {
                        parameters.Add(HttpProxyOptions.UseProxy, proxyOptions.UseProxy!);
                    }
                    if (!string.IsNullOrEmpty(proxyOptions.ProxyHost))
                    {
                        parameters.Add(HttpProxyOptions.ProxyHost, proxyOptions.ProxyHost!);
                    }
                    if (proxyOptions.ProxyPort.HasValue)
                    {
                        parameters.Add(HttpProxyOptions.ProxyPort, proxyOptions.ProxyPort.Value.ToString());
                    }
                    if (!string.IsNullOrEmpty(proxyOptions.ProxyAuth))
                    {
                        parameters.Add(HttpProxyOptions.ProxyAuth, proxyOptions.ProxyAuth!);
                    }
                    if (!string.IsNullOrEmpty(proxyOptions.ProxyUid))
                    {
                        parameters.Add(HttpProxyOptions.ProxyUID, proxyOptions.ProxyUid!);
                    }
                    if (!string.IsNullOrEmpty(proxyOptions.ProxyPwd))
                    {
                        parameters.Add(HttpProxyOptions.ProxyPWD, proxyOptions.ProxyPwd!);
                    }
                    if (!string.IsNullOrEmpty(proxyOptions.ProxyIgnoreList))
                    {
                        parameters.Add(HttpProxyOptions.ProxyIgnoreList, proxyOptions.ProxyIgnoreList!);
                    }
                }
            }

            return parameters;
        }

        internal SparkServerType ServerType => ((SparkConnection)Connection).ServerType;

        public override string VendorVersion => ((HiveServer2Connection)Connection).VendorVersion;

        public override bool SupportsDelete => false;

        public override bool SupportsUpdate => false;

        public override bool SupportCatalogName => false;

        public override bool ValidateAffectedRows => false;

        public override string GetInsertStatement(string tableName, string columnName, string? value) =>
            string.Format("INSERT INTO {0} ({1}) SELECT {2};", tableName, columnName, value ?? "NULL");

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
                            "CAST(2 as INTEGER) as int, " +
                            "CAST(1 as FLOAT) as number_float, " +
                            "CAST(4.56 as DOUBLE) as number_double, " +
                            "4.56BD as decimal, " +
                            "9.9999999999999999999999999999999999999BD as big_decimal, " +
                            "CAST(True as BOOLEAN) as is_active, " +
                            "'John Doe' as name, " +
                            "X'616263313233' as data, " +
                            "DATE '2023-09-08' as date, " +
                            "TIMESTAMP '2023-09-08 12:34:56+00:00' as timestamp, " +
                            "INTERVAL 178956969 YEAR 11 MONTH as interval, " +
                            "ARRAY(1, 2, 3) as numbers, " +
                            "STRUCT('John Doe' as name, 30 as age) as person," +
                            "MAP('name', CAST('Jane Doe' AS STRING), 'age', CAST(29 AS INT)) as map",
                    ExpectedValues =
                    [
                        new("id", typeof(long), typeof(Int64Type), 1L),
                        new("int", typeof(int), typeof(Int32Type), 2),
                        new("number_float", floatNetType, floatArrowType, floatValue),
                        new("number_double", typeof(double), typeof(DoubleType), 4.56d),
                        new("decimal", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("4.56")),
                        new("big_decimal", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9.9999999999999999999999999999999999999")),
                        new("is_active", typeof(bool), typeof(BooleanType), true),
                        new("name", typeof(string), typeof(StringType), "John Doe"),
                        new("data", typeof(byte[]), typeof(BinaryType), UTF8Encoding.UTF8.GetBytes("abc123")),
                        new("date", typeof(DateTime), typeof(Date32Type), new DateTime(2023, 9, 8)),
                        new("timestamp", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023, 9, 8, 12, 34, 56), TimeSpan.Zero)),
                        new("interval", typeof(string), typeof(StringType), "178956969-11"),
                        new("numbers", typeof(string), typeof(StringType), "[1,2,3]"),
                        new("person", typeof(string), typeof(StringType), """{"name":"John Doe","age":30}"""),
                        new("map", typeof(string), typeof(StringType), """{"age":"29","name":"Jane Doe"}""") // This is unexpected JSON. Expecting 29 to be a numeric and not string.
                    ]
                });

            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT  " +
                            "CAST(NULL as BIGINT) as id, " +
                            "CAST(NULL as INTEGER) as int, " +
                            "CAST(NULL as FLOAT) as number_float, " +
                            "CAST(NULL as DOUBLE) as number_double, " +
                            "CAST(NULL as DECIMAL(38,2)) as decimal,  " +
                            "CAST(NULL as BOOLEAN) as is_active, " +
                            "CAST(NULL as STRING)  as name, " +
                            "CAST(NULL as BINARY) as data, " +
                            "CAST(NULL as DATE) as date, " +
                            "CAST(NULL as TIMESTAMP) as timestamp," +
                            "CAST(NULL as MAP<STRING, INTEGER>) as map, " +
                            "CAST(NULL as ARRAY<INTEGER>) as numbers, " +
                            "CAST(NULL as STRUCT<field: STRING>) as person, " +
                            "MAP(CAST('EMPTY' as STRING), CAST(NULL as INTEGER)) as map_null, " +
                            "ARRAY(NULL,NULL,NULL) as numbers_null, " +
                            "STRUCT(CAST(NULL as STRING), CAST(NULL as INTEGER)) as person_null",
                    ExpectedValues =
                    [
                        new("id", typeof(long), typeof(Int64Type), null),
                        new("int", typeof(int), typeof(Int32Type), null),
                        new("number_float", floatNetType, floatArrowType, null),
                        new("number_double", typeof(double), typeof(DoubleType), null),
                        new("decimal", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new("is_active", typeof(bool), typeof(BooleanType), null),
                        new("name", typeof(string), typeof(StringType), null),
                        new("data", typeof(byte[]), typeof(BinaryType), null),
                        new("date", typeof(DateTime), typeof(Date32Type), null),
                        new("timestamp", typeof(DateTimeOffset), typeof(TimestampType), null),
                        new("map", typeof(string), typeof(StringType), null),
                        new("numbers", typeof(string), typeof(StringType), null),
                        new("person", typeof(string), typeof(StringType), null),
                        new("map_null", typeof(string), typeof(StringType), """{"EMPTY":null}"""),
                        new("numbers_null", typeof(string), typeof(StringType), """[null,null,null]"""),
                        new("person_null", typeof(string), typeof(StringType), """{"col1":null,"col2":null}"""),
                    ]
                });

            // complex struct
            sampleDataBuilder.Samples.Add(
            new SampleData()
            {
                Query = "SELECT " +
                         "STRUCT(" +
                         "\"Iron Man\" as name," +
                         "\"Avengers\" as team," +
                         "ARRAY(\"Genius\", \"Billionaire\", \"Playboy\", \"Philanthropist\") as powers," +
                         "ARRAY(" +
                         "  STRUCT(" +
                         "    \"Captain America\" as name, " +
                         "    \"Avengers\" as team, " +
                         "    ARRAY(\"Super Soldier Serum\", \"Vibranium Shield\") as powers, " +
                         "    ARRAY(" +
                         "     STRUCT(" +
                         "       \"Thanos\" as name, " +
                         "       \"Black Order\" as team, " +
                         "       ARRAY(\"Infinity Gauntlet\", \"Super Strength\", \"Teleportation\") as powers, " +
                         "       ARRAY(" +
                         "         STRUCT(" +
                         "           \"Loki\" as name, " +
                         "           \"Asgard\" as team, " +
                         "           ARRAY(\"Magic\", \"Shapeshifting\", \"Trickery\") as powers " +
                         "          )" +
                         "        ) as allies" +
                         "      )" +
                         "    ) as enemies" +
                         " )," +
                         " STRUCT(" +
                         "    \"Spider-Man\" as name, " +
                         "    \"Avengers\" as team, " +
                         "    ARRAY(\"Spider-Sense\", \"Web-Shooting\", \"Wall-Crawling\") as powers, " +
                         "    ARRAY(" +
                         "      STRUCT(" +
                         "        \"Green Goblin\" as name, " +
                         "        \"Sinister Six\" as team, " +
                         "        ARRAY(\"Glider\", \"Pumpkin Bombs\", \"Super Strength\") as powers, " +
                         "         ARRAY(" +
                         "          STRUCT(" +
                         "            \"Doctor Octopus\" as name, " +
                         "            \"Sinister Six\" as team, " +
                         "            ARRAY(\"Mechanical Arms\", \"Genius\", \"Madness\") as powers " +
                         "          )" +
                         "        ) as allies" +
                         "      )" +
                         "    ) as enemies" +
                         "  )" +
                         " ) as friends" +
                         ") as iron_man",
                ExpectedValues =
                   [
                        new("iron_man", typeof(string), typeof(StringType), "{\"name\":\"Iron Man\",\"team\":\"Avengers\",\"powers\":[\"Genius\",\"Billionaire\",\"Playboy\",\"Philanthropist\"],\"friends\":[{\"name\":\"Captain America\",\"team\":\"Avengers\",\"powers\":[\"Super Soldier Serum\",\"Vibranium Shield\"],\"enemies\":[{\"name\":\"Thanos\",\"team\":\"Black Order\",\"powers\":[\"Infinity Gauntlet\",\"Super Strength\",\"Teleportation\"],\"allies\":[{\"name\":\"Loki\",\"team\":\"Asgard\",\"powers\":[\"Magic\",\"Shapeshifting\",\"Trickery\"]}]}]},{\"name\":\"Spider-Man\",\"team\":\"Avengers\",\"powers\":[\"Spider-Sense\",\"Web-Shooting\",\"Wall-Crawling\"],\"enemies\":[{\"name\":\"Green Goblin\",\"team\":\"Sinister Six\",\"powers\":[\"Glider\",\"Pumpkin Bombs\",\"Super Strength\"],\"allies\":[{\"name\":\"Doctor Octopus\",\"team\":\"Sinister Six\",\"powers\":[\"Mechanical Arms\",\"Genius\",\"Madness\"]}]}]}]}")
                   ]
            });

            return sampleDataBuilder;
        }
    }
}
