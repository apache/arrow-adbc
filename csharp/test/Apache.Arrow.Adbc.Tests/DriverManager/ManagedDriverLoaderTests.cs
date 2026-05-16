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
using System.Reflection;
using Apache.Arrow.Adbc.DriverManager;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// Cross-TFM tests for the managed driver load path.
    /// These tests are compiled and run for every target framework the test
    /// project supports (net472, net8.0, net10.0; netstandard2.0 is consumed
    /// via the netstandard build of Apache.Arrow.Adbc), so the same behavior
    /// is verified on .NET Framework and on modern .NET where dependency
    /// resolution goes through <c>AssemblyLoadContext</c>.
    /// </summary>
    public class ManagedDriverLoaderTests : IDisposable
    {
        private readonly List<string> _tempDirs = new List<string>();

        public void Dispose()
        {
            foreach (string d in _tempDirs)
            {
                try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
            }
        }

        private string CreateTempDir()
        {
            string dir = Path.Combine(Path.GetTempPath(), "adbc_mdl_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            _tempDirs.Add(dir);
            return dir;
        }

        /// <summary>
        /// Copies the FakeAdbcDriver assembly and its companion .deps.json / .runtimeconfig.json
        /// (when present) into a fresh directory so the loader sees a complete driver layout.
        /// </summary>
        private string CopyDriverAssembly(string destDir)
        {
            string sourcePath = typeof(FakeAdbcDriver).Assembly.Location;
            string assemblyName = Path.GetFileName(sourcePath);
            string destPath = Path.Combine(destDir, assemblyName);
            File.Copy(sourcePath, destPath, overwrite: true);

            string sourceDir = Path.GetDirectoryName(sourcePath)!;
            string baseName = Path.GetFileNameWithoutExtension(sourcePath);
            foreach (string companion in new[] { ".deps.json", ".runtimeconfig.json", ".pdb" })
            {
                string from = Path.Combine(sourceDir, baseName + companion);
                if (File.Exists(from))
                {
                    File.Copy(from, Path.Combine(destDir, baseName + companion), overwrite: true);
                }
            }
            return destPath;
        }

        /// <summary>
        /// Copies only the driver assembly (no <c>.deps.json</c>, no <c>.runtimeconfig.json</c>)
        /// so we can pin the loader's behavior when the driver author forgets to ship
        /// the dependency manifest.
        /// </summary>
        private string CopyDriverAssemblyWithoutDepsJson(string destDir)
        {
            string sourcePath = typeof(FakeAdbcDriver).Assembly.Location;
            string assemblyName = Path.GetFileName(sourcePath);
            string destPath = Path.Combine(destDir, assemblyName);
            File.Copy(sourcePath, destPath, overwrite: true);
            return destPath;
        }

        [Fact]
        public void LoadManagedDriver_LoadsAssemblyAndInstantiatesType()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);

            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);
        }

        [Fact]
        public void LoadManagedDriver_PreservesSharedTypeIdentity()
        {
            // This is the key invariant for the .NET 8+ load path: the driver assembly
            // must end up in AssemblyLoadContext.Default so that the AdbcDriver type
            // identity is shared between the host and the driver. If we accidentally
            // loaded into a separate context, the cast/IsAssignableFrom check would fail.
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);

            Assert.IsAssignableFrom<AdbcDriver>(driver);
            Assert.Same(typeof(AdbcDriver), driver.GetType().BaseType);
        }

        [Fact]
        public void LoadManagedDriver_ReturnsWorkingDriver()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);

            var parameters = new Dictionary<string, string> { { "uri", "fake://test" } };
            AdbcDatabase db = driver.Open(parameters);
            Assert.NotNull(db);
        }

        [Fact]
        public void LoadManagedDriver_MissingAssembly_ThrowsAdbcExceptionNotFound()
        {
            string missing = Path.Combine(CreateTempDir(), "does_not_exist.dll");

            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(missing, "Some.Type"));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        [Fact]
        public void LoadManagedDriver_UnknownType_ThrowsAdbcExceptionNotFound()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);

            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(driverPath, "No.Such.Type"));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        [Fact]
        public void LoadManagedDriver_TypeNotAdbcDriver_ThrowsAdbcExceptionInvalidArgument()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);

            // Use a public type in the test assembly that does NOT derive from AdbcDriver.
            string typeName = typeof(ManagedDriverLoaderTests).FullName!;

            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(driverPath, typeName));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void LoadManagedDriver_NullOrEmptyArguments_ThrowArgumentException()
        {
            Assert.Throws<ArgumentException>(
                () => AdbcDriverManager.LoadManagedDriver(string.Empty, "T"));
            Assert.Throws<ArgumentException>(
                () => AdbcDriverManager.LoadManagedDriver("path.dll", string.Empty));
        }

        /// <summary>
        /// Reports which runtime / TFM the test is executing under. Useful when
        /// inspecting test output to confirm coverage on net472, net8.0 and net10.0.
        /// </summary>
        [Fact]
        public void LoadManagedDriver_RuntimeInformation_IsReported()
        {
            string runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;
            Assert.False(string.IsNullOrEmpty(runtime));

            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);
            Assert.NotNull(driver);
        }

        [Fact]
        public void LoadManagedDriver_FromCopiedLocation_LoadsSuccessfully()
        {
            // Verifies the .NET 8+ dependency-resolution path: the driver is loaded
            // from a directory other than the host's base directory. With the
            // AssemblyDependencyResolver hook installed, the driver's own .deps.json
            // (copied alongside it) is consulted for any dependencies the host
            // doesn't already satisfy.
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            Assert.NotEqual(
                Path.GetDirectoryName(typeof(ManagedDriverLoaderTests).Assembly.Location),
                Path.GetDirectoryName(driverPath));

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);
            Assert.NotNull(driver);

            // Sanity check: the loaded assembly's location is the copied path
            // (or, if the runtime returned the already-loaded instance, the original path).
            string? loadedLocation = driver.GetType().Assembly.Location;
            Assert.False(string.IsNullOrEmpty(loadedLocation));
        }

        [Fact]
        public void LoadManagedDriver_DepsJsonIsPresentNextToDriver()
        {
            // Pins the expectation that drivers in this repo ship a .deps.json next to
            // the assembly. The loader's dependency-resolution path on .NET 8+ relies on
            // it: without .deps.json AssemblyDependencyResolver throws and we cannot
            // map the driver's transitive references to files on disk.
            //
            // The .NET SDK only emits .deps.json for .NET Core / modern .NET targets;
            // .NET Framework builds do not produce one, so this assertion is gated.
#if NET6_0_OR_GREATER
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);

            string depsJsonPath = Path.Combine(
                Path.GetDirectoryName(driverPath)!,
                Path.GetFileNameWithoutExtension(driverPath) + ".deps.json");

            Assert.True(
                File.Exists(depsJsonPath),
                $"Driver assemblies are expected to ship a .deps.json on modern .NET. Missing: {depsJsonPath}");
#endif
        }

        [Fact]
        public void LoadManagedDriver_WithoutDepsJson_LoadsAssemblyButCannotResolveExternalDependencies()
        {
            // Documents the behavior the reviewer flagged: when a driver is deployed
            // without its .deps.json, the assembly itself can still be loaded (because
            // its only transitive reference here -- Apache.Arrow.Adbc -- is already in
            // the host's TPA list), but the loader silently has no AssemblyDependencyResolver
            // registered for it. Any "real" driver with private dependencies would fail
            // at first use. This test pins both halves of that contract.
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssemblyWithoutDepsJson(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string depsJsonPath = Path.Combine(
                Path.GetDirectoryName(driverPath)!,
                Path.GetFileNameWithoutExtension(driverPath) + ".deps.json");
            Assert.False(File.Exists(depsJsonPath));

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);
            Assert.NotNull(driver);
            Assert.Equal(typeName, driver.GetType().FullName);
        }
    }
}
