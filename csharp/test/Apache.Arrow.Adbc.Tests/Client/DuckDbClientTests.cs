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

using System.Data;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Client
{
    public class DuckDbClientTests : IClassFixture<DuckDbFixture>
    {
        readonly DuckDbFixture _duckDb;

        public DuckDbClientTests(DuckDbFixture duckDb)
        {
            _duckDb = duckDb;
        }

        [Fact]
        public void SimpleEndToEndTest()
        {
            using var connection = _duckDb.CreateConnection("clienttest.db", null);
            connection.Open();
            var command = connection.CreateCommand();

            command.CommandText = "CREATE TABLE integers(foo INTEGER, bar INTEGER);";
            int count = command.ExecuteNonQuery();

            command.CommandText = "INSERT INTO integers VALUES (3, 4), (5, 6), (7, 8);";
            count = command.ExecuteNonQuery();

            command.CommandText = "SELECT * from integers";
            using var reader = command.ExecuteReader();

            var schema = reader.GetSchemaTable();
            Assert.NotNull(schema);
            Assert.Equal(2, schema.Rows.Count);
            Assert.Equal(typeof(int), schema.Rows[0].ItemArray[2]);
            Assert.Equal(typeof(int), schema.Rows[1].ItemArray[2]);

            Assert.True(reader.Read());
            Assert.Equal(3, reader.GetInt32(0));
            Assert.True(reader.Read());
            Assert.Equal(5, reader.GetInt32(0));
            Assert.True(reader.Read());
            Assert.Equal(7, reader.GetInt32(0));
            Assert.False(reader.Read());
        }

        [Fact]
        public void TransactionsTest()
        {
            using var connection = _duckDb.CreateConnection("clienttransactions.db", null);
            connection.Open();
            var command = connection.CreateCommand();

            command.CommandText = "CREATE TABLE test(column1 INTEGER);";
            int count = command.ExecuteNonQuery();

            var transaction = connection.BeginTransaction();

            // Insert into connection1
            command.CommandText = "INSERT INTO test VALUES (3), (5), (7);";
            command.ExecuteUpdate();
            Assert.Equal(3, GetResultCount(command, "SELECT * from test"));

            // Validate that we don't see the data on connection2
            using var connection2 = _duckDb.CreateConnection("clienttransactions.db", null);
            connection2.Open();
            var command2 = connection2.CreateCommand();
            Assert.Equal(0, GetResultCount(command2, "SELECT * from test"));

            // ... until after a commit on connection1
            transaction.Commit();
            Assert.Equal(3, GetResultCount(command2, "SELECT * from test"));

            // When not in a transaction, we immediately see the results on another connection
            command.CommandText = "INSERT INTO test VALUES (2), (4);";
            command.ExecuteUpdate();
            Assert.Equal(5, GetResultCount(command2, "SELECT * from test"));

            transaction = connection.BeginTransaction();

            // Now you see it...
            command.CommandText = "INSERT INTO test VALUES (6);";
            command.ExecuteUpdate();
            Assert.Equal(6, GetResultCount(command, "SELECT * from test"));

            // Now you don't
            transaction.Rollback();
            Assert.Equal(5, GetResultCount(command, "SELECT * from test"));
        }

        [Fact]
        public void SetIsolationLevelFails()
        {
            using var connection = _duckDb.CreateConnection("clientisolation.db", null);
            connection.Open();

            Assert.ThrowsAny<AdbcException>(() =>
            {
                connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            });
        }

        [Fact]
        public void BindParameters()
        {
            using var connection = _duckDb.CreateConnection("bindparameters.db", null);
            connection.Open();
            var command = connection.CreateCommand();

            command.CommandText = "select ?, ?";
            command.Prepare();
            Assert.Equal(2, command.Parameters.Count);
            Assert.Equal(string.Empty, command.Parameters[0].ParameterName);
            Assert.Equal(DbType.Object, command.Parameters[0].DbType);
            Assert.Equal("1", command.Parameters[1].ParameterName);
            Assert.Equal(DbType.Object, command.Parameters[1].DbType);

            command.Parameters[0].DbType = DbType.Int32;
            command.Parameters[0].Value = 1;
            command.Parameters[1].DbType = DbType.String;
            command.Parameters[1].Value = "foo";

            using var reader = command.ExecuteReader();
            long count = 0;
            while (reader.Read())
            {
                count++;
            }
            Assert.Equal(1, count);
        }

        private static long GetResultCount(AdbcCommand command, string query)
        {
            command.CommandText = "SELECT * from test";
            using var reader = command.ExecuteReader();
            long count = 0;
            while (reader.Read())
            {
                count++;
            }
            return count;
        }
    }
}
