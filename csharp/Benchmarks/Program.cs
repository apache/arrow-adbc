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

using Apache.Arrow.Adbc.Tests;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace Apache.Arrow.Adbc.Benchmarks
{
    public class ClientBenchmark
    {
        public static DuckDbFixture? DuckDb;
        public static Client.AdbcConnection? Connection;

        static ClientBenchmark()
        {
            DuckDb = new DuckDbFixture();

            using (var database = DuckDb.OpenDatabase("test.db"))
            {
                using var connection = database.Connect(null);
                using var statement = connection.CreateStatement();

                statement.SqlQuery = "INSTALL tpch";
                statement.ExecuteUpdate();

                statement.SqlQuery = "LOAD tpch";
                statement.ExecuteUpdate();

                statement.SqlQuery = "CALL dbgen(sf = 1)";
                statement.ExecuteUpdate();
            }

            Connection = DuckDb.CreateConnection("test.db", null);
        }

        [Benchmark]
        public void Test()
        {
            using var command = Connection!.CreateCommand();
            command.CommandText = "SELECT * FROM lineitem";
            using var result = command.ExecuteReader();
            object[] row = new object[result.FieldCount];
            while (result.Read())
            {
                result.GetValues(row);
            }
        }
    }

    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                BenchmarkRunner.Run(typeof(Program).Assembly);
            }
            finally
            {
                ClientBenchmark.Connection?.Dispose();
                ClientBenchmark.DuckDb?.Dispose();
            }
        }
    }
}
