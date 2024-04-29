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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class ImportedDuckDbTests : IClassFixture<DuckDbFixture>
    {
        readonly DuckDbFixture _duckDb;

        public ImportedDuckDbTests(DuckDbFixture duckDb)
        {
            _duckDb = duckDb;
        }

        [Fact]
        public void SimpleEndToEndTest()
        {
            using var database = _duckDb.OpenDatabase("test.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE integers(foo INTEGER, bar INTEGER);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "INSERT INTO integers VALUES (3, 4), (5, 6), (7, 8);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "SELECT * from integers";
            var results = statement.ExecuteQuery();

            using var stream = results.Stream;

            var schema = stream.Schema;
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[1].DataType.TypeId);

            var firstBatch = stream.ReadNextRecordBatchAsync().Result;
            Assert.Equal(3, firstBatch.Length);
            Assert.Equal(3, (firstBatch.Column(0) as Int32Array).Values[0]);
            Assert.Equal(5, (firstBatch.Column(0) as Int32Array).Values[1]);
            Assert.Equal(7, (firstBatch.Column(0) as Int32Array).Values[2]);

            var secondBatch = stream.ReadNextRecordBatchAsync().Result;
            Assert.Null(secondBatch);
        }

        [Fact]
        public void ThisSub()
        {
            const string query = "ClwIARJYaHR0cHM6Ly9naXRodWIuY29tL3N1YnN0cmFpdC1pby9zdWJzdHJhaXQvYmxvYi9tYWluL2V4dGVuc2lvbnMvZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARABGgVlcXVhbBqgARKdAQp3EnUSSwpJEjoKCnByb2R1Y3RfaWQKDHByb2R1Y3RfbmFtZQoIcXVhbnRpdHkSFAoEOgIQAgoEYgIQAgoEKgIQAhgCOgsKCWludmVudG9yeRomGiQIARoECgIQAiIOGgwKCmIIQ29tcHV0ZXIiChoIEgYKBBICCAESDHByb2R1Y3RfbmFtZRIKcHJvZHVjdF9pZBIIcXVhbnRpdHkyEhAUKg52YWxpZGF0b3ItdGVzdA==";

            using var database = _duckDb.OpenDatabase("substraittest.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE inventory(product_id BIGINT, product_name STRING, quantity INTEGER);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "INSERT INTO inventory VALUES (3, 'Computer', 24), (4, 'Keyboard', 30);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "INSTALL substrait;";
            statement.ExecuteUpdate();

            statement.SqlQuery = "LOAD substrait;";
            statement.ExecuteUpdate();

            statement.SubstraitPlan = Convert.FromBase64String(query);
            var results = statement.ExecuteQuery();

            using var stream = results.Stream;

            var schema = stream.Schema;
            Assert.Equal(3, schema.FieldsList.Count);
            Assert.Equal(ArrowTypeId.Int64, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal(ArrowTypeId.String, schema.FieldsList[1].DataType.TypeId);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[2].DataType.TypeId);

            var firstBatch = stream.ReadNextRecordBatchAsync().Result;
            Assert.Equal(1, firstBatch.Length);
            //Assert.Equal(3, (firstBatch.Column(0) as Int32Array).Values[0]);
            //Assert.Equal(5, (firstBatch.Column(0) as Int32Array).Values[1]);
            //Assert.Equal(7, (firstBatch.Column(0) as Int32Array).Values[2]);

            var secondBatch = stream.ReadNextRecordBatchAsync().Result;
            Assert.Null(secondBatch);
        }
    }
}
