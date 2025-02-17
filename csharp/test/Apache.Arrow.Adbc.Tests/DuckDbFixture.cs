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
using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;
using DbClient = Apache.Arrow.Adbc.Client;

namespace Apache.Arrow.Adbc.Tests
{
    public class DuckDbFixture : IDisposable
    {
        readonly string _dataDirectory;
        readonly Dictionary<string, AdbcDatabase> _databases;
        readonly AdbcDriver _driver;
        bool _disposed;

        public DuckDbFixture()
        {
            _dataDirectory = Path.Combine(Path.GetTempPath(), "AdbcTest_DuckDb", Guid.NewGuid().ToString("D"));
            Directory.CreateDirectory(_dataDirectory);

            _databases = new Dictionary<string, AdbcDatabase>(StringComparer.OrdinalIgnoreCase);

            string root = Directory.GetCurrentDirectory();
            string file;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                file = Path.Combine(root, "libduckdb.so");
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                file = Path.Combine(root, "duckdb.dll");
            else
                file = Path.Combine(root, "libduckdb.dylib");

            _driver = CAdbcDriverImporter.Load(file, "duckdb_adbc_init");
        }

        public AdbcDatabase OpenDatabase(string name)
        {
            if (_driver == null) throw new ObjectDisposedException("DuckDbFixture");

            return _driver.Open(new Dictionary<string, string> { { "path", Path.Combine(_dataDirectory, name) } });
        }

        public DbClient.AdbcConnection CreateConnection(string name, IReadOnlyDictionary<string, string>? connectionOptions)
        {
            AdbcDatabase? database;
            if (!_databases.TryGetValue(name, out database))
            {
                database = OpenDatabase(name);
                _databases[name] = database;
            }

            var connection = database.Connect(connectionOptions);
            return new DbClient.AdbcConnection(_driver, database, connection);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _driver.Dispose();
                _disposed = true;

                try
                {
                    Directory.Delete(_dataDirectory, true);
                }
                catch
                {
                }
            }
        }
    }
}
