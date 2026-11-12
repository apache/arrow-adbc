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

using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class CAdbcDriverImporterFallbackTests
    {
        // When the library doesn't export the requested entrypoint, the fallback
        // entrypoint should be used. DuckDB only exports duckdb_adbc_init.
        [Fact]
        public void LoadWithFallback_MissingEntrypoint_UsesFallback()
        {
            string duckDb = Path.Combine(
                Directory.GetCurrentDirectory(),
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "duckdb.dll"
                : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "libduckdb.dylib"
                : "libduckdb.so");

            using AdbcDriver driver = CAdbcDriverImporter.LoadWithFallback(
                duckDb, "AdbcTestNoSuchEntrypoint", "duckdb_adbc_init");

            Assert.NotNull(driver);
        }
    }
}
