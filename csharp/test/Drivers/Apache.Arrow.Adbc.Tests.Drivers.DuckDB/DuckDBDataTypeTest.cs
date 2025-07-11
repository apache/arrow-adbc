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
using Apache.Arrow.Adbc.Drivers.DuckDB;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.DuckDB
{
    public class DuckDBDataTypeTest : IAsyncLifetime
    {
        private DuckDBDriver? _driver;
        private AdbcDatabase? _database;
        private AdbcConnection? _connection;

        public async Task InitializeAsync()
        {
            _driver = new DuckDBDriver();
            _database = _driver.Open(new Dictionary<string, string>
            {
                { DuckDBParameters.DataSource, ":memory:" }
            });
            _connection = _database.Connect(null);
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            _connection?.Dispose();
            _database?.Dispose();
            await Task.CompletedTask;
        }

        [Fact]
        public async Task TestNumericTypes()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = @"
                SELECT 
                    CAST(1 AS BOOLEAN) as bool_val,
                    CAST(127 AS TINYINT) as tinyint_val,
                    CAST(32767 AS SMALLINT) as smallint_val,
                    CAST(2147483647 AS INTEGER) as int_val,
                    CAST(9223372036854775807 AS BIGINT) as bigint_val,
                    CAST(255 AS UTINYINT) as utinyint_val,
                    CAST(65535 AS USMALLINT) as usmallint_val,
                    CAST(4294967295 AS UINTEGER) as uint_val,
                    CAST(18446744073709551615 AS UBIGINT) as ubigint_val,
                    CAST(3.14 AS FLOAT) as float_val,
                    CAST(3.14159265359 AS DOUBLE) as double_val";

            var result = await statement.ExecuteQueryAsync();
            await using var stream = result.Stream;
            var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            
            // Verify types
            Assert.IsType<BooleanType>(batch!.Schema.GetFieldByIndex(0).DataType);
            Assert.IsType<Int8Type>(batch.Schema.GetFieldByIndex(1).DataType);
            Assert.IsType<Int16Type>(batch.Schema.GetFieldByIndex(2).DataType);
            Assert.IsType<Int32Type>(batch.Schema.GetFieldByIndex(3).DataType);
            Assert.IsType<Int64Type>(batch.Schema.GetFieldByIndex(4).DataType);
            Assert.IsType<UInt8Type>(batch.Schema.GetFieldByIndex(5).DataType);
            Assert.IsType<UInt16Type>(batch.Schema.GetFieldByIndex(6).DataType);
            Assert.IsType<UInt32Type>(batch.Schema.GetFieldByIndex(7).DataType);
            Assert.IsType<UInt64Type>(batch.Schema.GetFieldByIndex(8).DataType);
            Assert.IsType<FloatType>(batch.Schema.GetFieldByIndex(9).DataType);
            Assert.IsType<DoubleType>(batch.Schema.GetFieldByIndex(10).DataType);

            // Verify values
            Assert.True(((BooleanArray)batch.Column(0)).GetValue(0));
            Assert.Equal((sbyte)127, ((Int8Array)batch.Column(1)).GetValue(0));
            Assert.Equal((short)32767, ((Int16Array)batch.Column(2)).GetValue(0));
            Assert.Equal(2147483647, ((Int32Array)batch.Column(3)).GetValue(0));
            Assert.Equal(9223372036854775807L, ((Int64Array)batch.Column(4)).GetValue(0));
            Assert.Equal((byte)255, ((UInt8Array)batch.Column(5)).GetValue(0));
            Assert.Equal((ushort)65535, ((UInt16Array)batch.Column(6)).GetValue(0));
            Assert.Equal(4294967295U, ((UInt32Array)batch.Column(7)).GetValue(0));
            Assert.Equal(18446744073709551615UL, ((UInt64Array)batch.Column(8)).GetValue(0));
            Assert.Equal(3.14f, ((FloatArray)batch.Column(9)).GetValue(0), 4);
            Assert.Equal(3.14159265359, ((DoubleArray)batch.Column(10)).GetValue(0), 10);
        }

        [Fact]
        public async Task TestStringAndBinaryTypes()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = @"
                SELECT 
                    'Hello, World!' as string_val,
                    CAST('Binary data' AS BLOB) as binary_val,
                    gen_random_uuid() as uuid_val";

            var result = await statement.ExecuteQueryAsync();
            await using var stream = result.Stream;
            var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            
            // Verify types
            Assert.IsType<StringType>(batch!.Schema.GetFieldByIndex(0).DataType);
            Assert.IsType<BinaryType>(batch.Schema.GetFieldByIndex(1).DataType);
            Assert.IsType<FixedSizeBinaryType>(batch.Schema.GetFieldByIndex(2).DataType);
            
            var uuidType = (FixedSizeBinaryType)batch.Schema.GetFieldByIndex(2).DataType;
            Assert.Equal(16, uuidType.ByteWidth);

            // Verify values
            Assert.Equal("Hello, World!", ((StringArray)batch.Column(0)).GetString(0));
            
            var binaryArray = (BinaryArray)batch.Column(1);
            var binaryData = binaryArray.GetBytes(0).ToArray();
            Assert.Equal("Binary data", System.Text.Encoding.UTF8.GetString(binaryData));
            
            var uuidArray = (FixedSizeBinaryArray)batch.Column(2);
            var uuidBytes = uuidArray.GetBytes(0);
            Assert.Equal(16, uuidBytes.Length);
        }

        [Fact]
        public async Task TestDateTimeTypes()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = @"
                SELECT 
                    DATE '2024-01-15' as date_val,
                    TIME '14:30:45.123456' as time_val,
                    TIMESTAMP '2024-01-15 14:30:45.123456' as timestamp_val";

            var result = await statement.ExecuteQueryAsync();
            await using var stream = result.Stream;
            var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            
            // Verify types
            Assert.IsType<Date32Type>(batch!.Schema.GetFieldByIndex(0).DataType);
            Assert.IsType<Time64Type>(batch.Schema.GetFieldByIndex(1).DataType);
            Assert.IsType<TimestampType>(batch.Schema.GetFieldByIndex(2).DataType);
            
            var timeType = (Time64Type)batch.Schema.GetFieldByIndex(1).DataType;
            Assert.Equal(TimeUnit.Microsecond, timeType.Unit);
            
            var timestampType = (TimestampType)batch.Schema.GetFieldByIndex(2).DataType;
            Assert.Equal(TimeUnit.Microsecond, timestampType.Unit);
        }

        [Fact]
        public async Task TestDecimalType()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "SELECT CAST(123.45 AS DECIMAL(10,2)) as decimal_val";

            var result = await statement.ExecuteQueryAsync();
            await using var stream = result.Stream;
            var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            
            // Verify type
            Assert.IsType<Decimal128Type>(batch!.Schema.GetFieldByIndex(0).DataType);
            
            var decimalArray = (Decimal128Array)batch.Column(0);
            var value = decimalArray.GetValue(0);
            Assert.Equal(123.45m, value);
        }

        [Fact]
        public async Task TestNullHandling()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = @"
                SELECT 
                    NULL::INTEGER as null_int,
                    NULL::VARCHAR as null_string,
                    NULL::DOUBLE as null_double,
                    NULL::DATE as null_date,
                    NULL::BLOB as null_binary";

            var result = await statement.ExecuteQueryAsync();
            await using var stream = result.Stream;
            var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            Assert.Equal(1, batch!.Length);
            
            // All values should be null
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                Assert.True(batch.Column(i).IsNull(0));
            }
        }

        [Fact]
        public async Task TestMixedNullAndNonNull()
        {
            using var statement = _connection!.CreateStatement();
            
            // Create test data with mix of nulls and values
            statement.SqlQuery = @"
                CREATE TABLE test_nulls AS 
                SELECT * FROM (VALUES 
                    (1, 'one', 1.1),
                    (NULL, 'two', 2.2),
                    (3, NULL, 3.3),
                    (4, 'four', NULL),
                    (NULL, NULL, NULL)
                ) t(int_col, str_col, dbl_col)";
            await statement.ExecuteUpdateAsync();

            statement.SqlQuery = "SELECT * FROM test_nulls ORDER BY rowid";
            var result = await statement.ExecuteQueryAsync();
            await using var stream = result.Stream;
            var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            Assert.Equal(5, batch!.Length);
            
            var intArray = (Int32Array)batch.Column(0);
            var strArray = (StringArray)batch.Column(1);
            var dblArray = (DoubleArray)batch.Column(2);
            
            // Row 0: all non-null
            Assert.False(intArray.IsNull(0));
            Assert.Equal(1, intArray.GetValue(0));
            Assert.False(strArray.IsNull(0));
            Assert.Equal("one", strArray.GetString(0));
            Assert.False(dblArray.IsNull(0));
            Assert.Equal(1.1, dblArray.GetValue(0));
            
            // Row 1: int is null
            Assert.True(intArray.IsNull(1));
            Assert.False(strArray.IsNull(1));
            Assert.Equal("two", strArray.GetString(1));
            Assert.False(dblArray.IsNull(1));
            Assert.Equal(2.2, dblArray.GetValue(1));
            
            // Row 2: string is null
            Assert.False(intArray.IsNull(2));
            Assert.Equal(3, intArray.GetValue(2));
            Assert.True(strArray.IsNull(2));
            Assert.False(dblArray.IsNull(2));
            Assert.Equal(3.3, dblArray.GetValue(2));
            
            // Row 3: double is null
            Assert.False(intArray.IsNull(3));
            Assert.Equal(4, intArray.GetValue(3));
            Assert.False(strArray.IsNull(3));
            Assert.Equal("four", strArray.GetString(3));
            Assert.True(dblArray.IsNull(3));
            
            // Row 4: all null
            Assert.True(intArray.IsNull(4));
            Assert.True(strArray.IsNull(4));
            Assert.True(dblArray.IsNull(4));
        }
    }
}