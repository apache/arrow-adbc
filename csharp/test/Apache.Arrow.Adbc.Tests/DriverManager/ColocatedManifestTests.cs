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
    /// Tests for co-located manifest file auto-discovery.
    /// </summary>
    public class ColocatedManifestTests : IDisposable
    {
        private readonly List<string> _tempFiles = new List<string>();
        private readonly List<string> _tempDirs = new List<string>();

        private (string dllPath, string tomlPath) CreateTestFilePair(string baseName, string tomlContent)
        {
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string dllPath = Path.Combine(tempDir, baseName + ".dll");
            string tomlPath = Path.Combine(tempDir, baseName + ".toml");

            File.WriteAllText(dllPath, "fake dll content");
            File.WriteAllText(tomlPath, tomlContent);

            _tempFiles.Add(dllPath);
            _tempFiles.Add(tomlPath);

            return (dllPath, tomlPath);
        }

        public void Dispose()
        {
            foreach (string f in _tempFiles)
            {
                try { if (File.Exists(f)) File.Delete(f); } catch { }
            }
            foreach (string d in _tempDirs)
            {
                try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
            }
        }

        [Fact]
        public void LoadDriver_WithColocatedManifest_LoadsFromManifest()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"
                + "\n[options]\n"
                + "from_manifest = \"true\"\n"
                + "manifest_version = \"1.0\"\n";

            var (dllPath, tomlPath) = CreateTestFilePair("test_driver", toml);

            // LoadDriver should auto-detect the co-located manifest and use it to determine:
            // - The actual driver location (from the 'driver' field)
            // - Whether it's a managed driver (from 'driver_type')
            // - The entrypoint (if specified)
            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath);
            Assert.NotNull(driver);
            Assert.IsType<FakeAdbcDriver>(driver);

            // NOTE: Manifest options are stored in the profile but NOT automatically applied here.
            // To use manifest options, explicitly load the profile and use OpenDatabaseFromProfile,
            // or pass options when opening the database.
            var db = driver.Open(new Dictionary<string, string> { { "test_key", "test_value" } });
            FakeAdbcDatabase fakeDb = Assert.IsType<FakeAdbcDatabase>(db);
            Assert.Equal("test_value", fakeDb.Parameters["test_key"]);
        }

        [Fact]
        public void LoadDriver_WithoutColocatedManifest_FailsAsExpected()
        {
            // Create a DLL without a co-located manifest
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string dllPath = Path.Combine(tempDir, "no_manifest.dll");
            File.WriteAllText(dllPath, "fake dll");
            _tempFiles.Add(dllPath);

            // Should fail because the DLL doesn't exist as a real native library
            // (but importantly, it should NOT fail looking for a manifest)
            Assert.ThrowsAny<Exception>(() => AdbcDriverManager.LoadDriver(dllPath));
        }

        [Fact]
        public void FindLoadDriver_WithColocatedManifest_UsesManifest()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"  // Important: Must specify driver_type for managed drivers
                + "\n[options]\n"
                + "auto_discovered = \"yes\"\n";

            var (dllPath, tomlPath) = CreateTestFilePair("my_driver", toml);

            // FindLoadDriver should auto-detect co-located manifest and use it to load the driver
            // NOTE: Options from the manifest are NOT automatically applied - they're only available
            // when using OpenDatabaseFromProfile. The manifest is primarily for specifying HOW to
            // load the driver (driver path, driver_type, entrypoint), not database configuration.
            AdbcDriver driver = AdbcDriverManager.FindLoadDriver(dllPath);
            Assert.NotNull(driver);
            Assert.IsType<FakeAdbcDriver>(driver);

            // To actually use the manifest options, you would need to:
            // 1. Load the profile separately with TomlConnectionProfile.FromFile(tomlPath)
            // 2. Use AdbcDriverManager.OpenDatabaseFromProfile(profile)
            // Or just pass options when opening the database
            var db = driver.Open(new Dictionary<string, string> { { "manual_option", "value" } });
            FakeAdbcDatabase fakeDb = Assert.IsType<FakeAdbcDatabase>(db);
            Assert.Equal("value", fakeDb.Parameters["manual_option"]);
        }

        [Fact]
        public void LoadDriver_ManifestCanOverrideDriverPath()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            // Manifest points to the actual driver assembly
            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n";

            // DLL file is just a placeholder - the manifest redirects to the real driver
            var (dllPath, tomlPath) = CreateTestFilePair("placeholder", toml);

            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath);
            Assert.NotNull(driver);
            Assert.IsType<FakeAdbcDriver>(driver);
        }

        [Fact]
        public void LoadDriver_ExplicitEntrypointStillWorks()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n";

            var (dllPath, tomlPath) = CreateTestFilePair("entrypoint_test", toml);

            // Even with a manifest, explicit entrypoint parameter should work
            // (though for managed drivers, entrypoint doesn't apply - it's ignored)
            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath, "CustomEntrypoint");
            Assert.NotNull(driver);
            Assert.IsType<FakeAdbcDriver>(driver);
        }

        [Fact]
        public void LoadDriver_RelativePathInManifest_ResolvedCorrectly()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;
            string assemblyFileName = Path.GetFileName(assemblyPath);

            // Copy the assembly to the test directory so we can use a relative path
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string localAssemblyPath = Path.Combine(tempDir, assemblyFileName);
            File.Copy(assemblyPath, localAssemblyPath, overwrite: true);
            _tempFiles.Add(localAssemblyPath);

            // Manifest uses relative path to the driver
            string toml = "version = 1\n"
                + "driver = \"" + assemblyFileName + "\"\n"
                + "driver_type = \"" + typeName + "\"\n";

            string dllPath = Path.Combine(tempDir, "wrapper.dll");
            string tomlPath = Path.Combine(tempDir, "wrapper.toml");

            File.WriteAllText(dllPath, "fake");
            File.WriteAllText(tomlPath, toml);

            _tempFiles.Add(dllPath);
            _tempFiles.Add(tomlPath);

            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath);
            Assert.NotNull(driver);
            Assert.IsType<FakeAdbcDriver>(driver);
        }

        [Fact]
        public void LoadDriver_DifferentExtensions_AllDetectManifest()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n";

            // Test with .dll extension
            var (dll1, _) = CreateTestFilePair("test.driver", toml);
            AdbcDriver driver1 = AdbcDriverManager.LoadDriver(dll1);
            Assert.NotNull(driver1);
            Assert.IsType<FakeAdbcDriver>(driver1);

            // Test with .so extension
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string soPath = Path.Combine(tempDir, "test.driver.so");
            string soToml = Path.Combine(tempDir, "test.driver.toml");
            File.WriteAllText(soPath, "fake");
            File.WriteAllText(soToml, toml);
            _tempFiles.Add(soPath);
            _tempFiles.Add(soToml);

            // Should auto-detect manifest even with .so extension
            AdbcDriver driver2 = AdbcDriverManager.LoadDriver(soPath);
            Assert.NotNull(driver2);
            Assert.IsType<FakeAdbcDriver>(driver2);
        }
    }
}
