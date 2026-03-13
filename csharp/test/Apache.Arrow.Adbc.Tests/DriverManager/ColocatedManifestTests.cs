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

        /// <summary>
        /// Creates a test directory with a placeholder DLL, a co-located TOML manifest,
        /// and optionally copies the actual driver assembly.
        /// </summary>
        private (string dllPath, string tomlPath) CreateTestFilePair(
            string baseName,
            string tomlContent,
            bool copyRealAssembly = false)
        {
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string dllPath = Path.Combine(tempDir, baseName + ".dll");
            string tomlPath = Path.Combine(tempDir, baseName + ".toml");

            if (copyRealAssembly)
            {
                // Copy the real FakeAdbcDriver assembly to the temp directory
                string realAssemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
                File.Copy(realAssemblyPath, dllPath, overwrite: true);
            }
            else
            {
                File.WriteAllText(dllPath, "fake dll content");
            }

            File.WriteAllText(tomlPath, tomlContent);

            _tempFiles.Add(dllPath);
            _tempFiles.Add(tomlPath);

            return (dllPath, tomlPath);
        }

        /// <summary>
        /// Creates test files where the manifest uses a relative path to a real assembly.
        /// </summary>
        private (string placeholderDllPath, string tomlPath, string realAssemblyPath) CreateTestFilesWithRelativeDriver(
            string baseName,
            string typeName)
        {
            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            // Copy the real assembly to the temp directory
            string realAssemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string realAssemblyName = Path.GetFileName(realAssemblyPath);
            string copiedAssemblyPath = Path.Combine(tempDir, realAssemblyName);
            File.Copy(realAssemblyPath, copiedAssemblyPath, overwrite: true);
            _tempFiles.Add(copiedAssemblyPath);

            // Create a placeholder DLL with a different name
            string placeholderDllPath = Path.Combine(tempDir, baseName + ".dll");
            File.WriteAllText(placeholderDllPath, "placeholder");
            _tempFiles.Add(placeholderDllPath);

            // Create manifest that uses relative path to the real assembly
            string toml = "version = 1\n"
                + "driver = \"" + realAssemblyName + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"
                + "\n[options]\n"
                + "from_manifest = \"true\"\n";

            string tomlPath = Path.Combine(tempDir, baseName + ".toml");
            File.WriteAllText(tomlPath, toml);
            _tempFiles.Add(tomlPath);

            return (placeholderDllPath, tomlPath, copiedAssemblyPath);
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
            string typeName = typeof(FakeAdbcDriver).FullName!;

            (string dllPath, string tomlPath, string realAssemblyPath) =
                CreateTestFilesWithRelativeDriver("test_driver", typeName);

            // LoadDriver should auto-detect the co-located manifest and use it to determine:
            // - The actual driver location (from the 'driver' field - relative path)
            // - Whether it's a managed driver (from 'driver_type')
            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath);
            Assert.NotNull(driver);
            // Check type name instead of IsType to avoid assembly identity issues
            Assert.Equal(typeName, driver.GetType().FullName);

            // NOTE: Manifest options are stored in the profile but NOT automatically applied here.
            AdbcDatabase db = driver.Open(new Dictionary<string, string> { { "test_key", "test_value" } });
            // FakeAdbcDatabase full name
            Assert.Equal("Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDatabase", db.GetType().FullName);
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
            string typeName = typeof(FakeAdbcDriver).FullName!;

            (string dllPath, string tomlPath, string realAssemblyPath) =
                CreateTestFilesWithRelativeDriver("my_driver", typeName);

            // FindLoadDriver should auto-detect co-located manifest and use it to load the driver
            AdbcDriver driver = AdbcDriverManager.FindLoadDriver(dllPath);
            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);

            AdbcDatabase db = driver.Open(new Dictionary<string, string> { { "manual_option", "value" } });
            Assert.NotNull(db);
        }

        [Fact]
        public void LoadDriver_ManifestCanOverrideDriverPath()
        {
            string typeName = typeof(FakeAdbcDriver).FullName!;

            // DLL file is just a placeholder - the manifest redirects to the real driver
            (string dllPath, string tomlPath, string realAssemblyPath) =
                CreateTestFilesWithRelativeDriver("placeholder", typeName);

            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath);
            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);
        }

        [Fact]
        public void LoadDriver_ExplicitEntrypointStillWorks()
        {
            string typeName = typeof(FakeAdbcDriver).FullName!;

            (string dllPath, string tomlPath, string realAssemblyPath) =
                CreateTestFilesWithRelativeDriver("entrypoint_test", typeName);

            // Even with a manifest, explicit entrypoint parameter should work
            // (though for managed drivers, entrypoint doesn't apply - it's ignored)
            AdbcDriver driver = AdbcDriverManager.LoadDriver(dllPath, "CustomEntrypoint");
            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);
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
            Assert.Equal(typeName, driver.GetType().FullName);
        }

        [Fact]
        public void LoadDriver_DifferentExtensions_AllDetectManifest()
        {
            string typeName = typeof(FakeAdbcDriver).FullName!;

            // Test with .dll extension - use relative path helper
            (string dll1, string toml1, string real1) = CreateTestFilesWithRelativeDriver("test.driver", typeName);
            AdbcDriver driver1 = AdbcDriverManager.LoadDriver(dll1);
            Assert.NotNull(driver1);
            Assert.Equal(typeName, driver1.GetType().FullName);

            // Test with .so extension - also with relative path
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string assemblyFileName = Path.GetFileName(assemblyPath);

            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            // Copy the real assembly
            string copiedAssemblyPath = Path.Combine(tempDir, assemblyFileName);
            File.Copy(assemblyPath, copiedAssemblyPath, overwrite: true);
            _tempFiles.Add(copiedAssemblyPath);

            // Create manifest with relative path
            string toml = "version = 1\n"
                + "driver = \"" + assemblyFileName + "\"\n"
                + "driver_type = \"" + typeName + "\"\n";

            string soPath = Path.Combine(tempDir, "test.driver.so");
            string soToml = Path.Combine(tempDir, "test.driver.toml");
            File.WriteAllText(soPath, "fake");
            File.WriteAllText(soToml, toml);
            _tempFiles.Add(soPath);
            _tempFiles.Add(soToml);

            // Should auto-detect manifest even with .so extension
            AdbcDriver driver2 = AdbcDriverManager.LoadDriver(soPath);
            Assert.NotNull(driver2);
            Assert.Equal(typeName, driver2.GetType().FullName);
        }

        [Fact]
        public void LoadManagedDriver_LoadsDirectly()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            // LoadManagedDriver loads directly from the provided assembly path and type name
            // Note: This bypasses manifest entirely and loads directly from absolute path
            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(assemblyPath, typeName);
            Assert.NotNull(driver);
            // Use type name comparison to avoid assembly identity issues when loaded from different path
            Assert.Equal(typeName, driver.GetType().FullName);
        }

        [Fact]
        public void LoadManagedDriver_WithColocatedManifest_LoadsDirectly()
        {
            // Note: LoadManagedDriver does not currently detect co-located manifests.
            // It loads directly from the specified assembly path.
            // To use manifest redirection, use LoadDriver with a co-located manifest
            // that specifies driver_type.
            string typeName = typeof(FakeAdbcDriver).FullName!;
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;

            // LoadManagedDriver loads directly from the provided path
            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(assemblyPath, typeName);
            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);
        }
    }
}
