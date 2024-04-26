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
    }
}
