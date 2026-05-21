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
using Apache.Arrow.Adbc.DriverManager;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// Tests for the entrypoint-scheme dispatch in <see cref="AdbcDriverManager"/>:
    /// <c>dotnet:</c> and <c>netfx:</c> prefixes select the managed loader and
    /// fail closed when the host process runs on the wrong .NET runtime.
    /// </summary>
    [Collection(DriverManagerSecurityCollection.Name)]
    public class EntrypointSchemeTests : IDisposable
    {
        private readonly List<string> _tempDirs = new List<string>();

        public void Dispose()
        {
            foreach (string d in _tempDirs)
            {
                try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
            }
        }

        /// <summary>The "other" managed scheme for this test process (the one the host can't load).</summary>
        private static string ForeignScheme =>
#if NETFRAMEWORK
            "dotnet:";
#else
            "netfx:";
#endif

        /// <summary>The matching managed scheme for this test process.</summary>
        private static string NativeScheme =>
#if NETFRAMEWORK
            "netfx:";
#else
            "dotnet:";
#endif

        private string CreateManifest(string entrypoint, string assemblyFileName)
        {
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string realAssemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            File.Copy(realAssemblyPath, Path.Combine(tempDir, assemblyFileName), overwrite: true);

            string toml = "manifest_version = 1\n"
                + "\n[Driver]\n"
                + "entrypoint = \"" + entrypoint + "\"\n"
                + "shared = \"" + assemblyFileName + "\"\n";

            string tomlPath = Path.Combine(tempDir, "scheme_test.toml");
            File.WriteAllText(tomlPath, toml);
            return tomlPath;
        }

        [Fact]
        public void Manifest_ForeignScheme_FailsClosedWithClearError()
        {
            // A dotnet: manifest under .NET Framework (or netfx: under modern .NET)
            // should fail before any reflection happens, with a message that names
            // the requested scheme.
            string assemblyFileName = Path.GetFileName(typeof(FakeAdbcDriver).Assembly.Location);
            string typeName = typeof(FakeAdbcDriver).FullName!;
            string entrypoint = ForeignScheme + typeName;
            string tomlPath = CreateManifest(entrypoint, assemblyFileName);

            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.FindLoadDriver(tomlPath));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
            Assert.Contains(ForeignScheme.TrimEnd(':'), ex.Message);
        }

        [Fact]
        public void Manifest_NativeScheme_LoadsManagedDriver()
        {
            // Sanity check: the matching scheme on the same code path does load.
            string assemblyFileName = Path.GetFileName(typeof(FakeAdbcDriver).Assembly.Location);
            string typeName = typeof(FakeAdbcDriver).FullName!;
            string entrypoint = NativeScheme + typeName;
            string tomlPath = CreateManifest(entrypoint, assemblyFileName);

            AdbcDriver driver = AdbcDriverManager.FindLoadDriver(tomlPath);
            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);
        }

        [Fact]
        public void LoadDriver_ExplicitForeignSchemeEntrypoint_FailsClosed()
        {
            // The dispatch fires even without a manifest: an explicit caller-supplied
            // entrypoint with a foreign scheme prefix is rejected on this runtime.
            string realAssemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadDriver(realAssemblyPath, ForeignScheme + typeName));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
        }
    }
}
