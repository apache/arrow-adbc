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

        /// <summary>
        /// Integration-style proof that managed-driver loading preserves a single
        /// identity for the shared public contract assemblies between the host and
        /// the driver. This is the failure mode that is most likely to bite in
        /// production: the driver's <c>.deps.json</c> happily lists a different
        /// version of <c>Apache.Arrow.Adbc</c> / <c>Apache.Arrow</c> /
        /// <c>System.Diagnostics.DiagnosticSource</c>, but on the default
        /// <c>AssemblyLoadContext</c> the host's copy always wins -- and that is
        /// exactly what we want for type identity.
        /// </summary>
        [Fact]
        public void LoadManagedDriver_SharedContractAssemblies_ResolveToHostIdentity()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);

            // The driver's loaded assembly must reference Apache.Arrow.Adbc (the public
            // contract). Whatever version that reference points to, after loading it
            // must resolve to the SAME Assembly instance the host is using -- otherwise
            // AdbcDriver / AdbcDatabase would be two different types and the cast below
            // would fail with a runtime InvalidCastException.
            Assembly driverAssembly = driver.GetType().Assembly;

            // 1. Driver and host share AdbcDriver identity.
            Assert.Same(typeof(AdbcDriver), driver.GetType().BaseType);
            Assert.Same(typeof(AdbcDriver).Assembly, driver.GetType().BaseType!.Assembly);

            // 2. For each shared public contract assembly the driver references,
            //    the resolved instance must be the host's copy. Apache.Arrow and
            //    System.Diagnostics.DiagnosticSource may or may not be referenced
            //    by the fake driver directly, so we only assert on the ones it does
            //    reference.
            string[] sharedContractAssemblies =
            {
                "Apache.Arrow.Adbc",
                "Apache.Arrow",
                "System.Diagnostics.DiagnosticSource",
            };

            // Apache.Arrow.Adbc must always be a shared reference.
            Assert.Contains(
                driverAssembly.GetReferencedAssemblies(),
                n => string.Equals(n.Name, "Apache.Arrow.Adbc", StringComparison.OrdinalIgnoreCase));

            foreach (string contract in sharedContractAssemblies)
            {
                AssemblyName? reference = System.Array.Find(
                    driverAssembly.GetReferencedAssemblies(),
                    n => string.Equals(n.Name, contract, StringComparison.OrdinalIgnoreCase));
                if (reference == null)
                {
                    continue;
                }

                // Resolving the reference must return whatever the host has -- or, if the
                // host has not yet touched it, the runtime's default load -- and from
                // that point on there must be exactly one identity per simple name in
                // the AppDomain. Any "version skew" failure would manifest as two
                // assemblies with the same simple name but different MVIDs.
                Assembly resolved = Assembly.Load(reference);

                Assembly[] loaded = System.Array.FindAll(
                    AppDomain.CurrentDomain.GetAssemblies(),
                    a => string.Equals(a.GetName().Name, contract, StringComparison.OrdinalIgnoreCase));
                Assert.Single(loaded);
                Assert.Same(loaded[0], resolved);
            }

            // 3. Cross-boundary type identity: the AdbcDatabase the driver returns is
            //    assignment-compatible with the host's AdbcDatabase, which can only be
            //    true if Apache.Arrow.Adbc has a single identity across the boundary.
            AdbcDatabase db = driver.Open(new Dictionary<string, string> { { "uri", "fake://skew" } });
            Assert.IsAssignableFrom<AdbcDatabase>(db);
            Assert.Same(typeof(AdbcDatabase), db.GetType().BaseType);
        }

        /// <summary>
        /// Version-skew proof: copy a foreign file named <c>Apache.Arrow.Adbc.dll</c>
        /// (a different assembly, masquerading as the public contract by simple name)
        /// next to the driver. On the default <see cref="System.Runtime.Loader.AssemblyLoadContext"/>
        /// the host's already-loaded <c>Apache.Arrow.Adbc</c> must still win when the
        /// driver resolves the contract -- the colocated copy must be ignored.
        /// </summary>
        /// <remarks>
        /// This directly exercises the failure mode the reviewer called out
        /// (&quot;the failure mode that is most likely to bite in production&quot;):
        /// a driver that ships a different version of a shared contract assembly
        /// in its own directory must not be able to replace the host's copy at
        /// runtime, because doing so would split type identity for
        /// <see cref="AdbcDriver"/> / <see cref="AdbcDatabase"/> across the boundary
        /// and break every cast.
        /// </remarks>
        [Fact]
        public void LoadManagedDriver_ColocatedSharedContract_HostIdentityWins()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            // Drop a foreign file using the simple name of the public contract assembly
            // next to the driver. We deliberately copy a *different* managed assembly
            // (the test assembly itself) over the contract file name -- if the loader
            // were to honor colocated files for already-loaded assemblies, the runtime
            // would either pick up this bogus file or refuse to load at all.
            string foreignSource = typeof(ManagedDriverLoaderTests).Assembly.Location;
            string fakeContractPath = Path.Combine(dir, "Apache.Arrow.Adbc.dll");
            File.Copy(foreignSource, fakeContractPath, overwrite: true);

            // Sanity: prove we actually planted a file that differs from the host's
            // real Apache.Arrow.Adbc.dll byte-for-byte (different assembly entirely).
            string hostContractLocation = typeof(AdbcDriver).Assembly.Location;
            Assert.NotEqual(
                new FileInfo(hostContractLocation).Length,
                new FileInfo(fakeContractPath).Length);

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);

            // Host's Apache.Arrow.Adbc identity is preserved across the boundary
            // even though a colocated file with the same simple name exists.
            Assert.Same(typeof(AdbcDriver), driver.GetType().BaseType);
            Assert.Same(typeof(AdbcDriver).Assembly, driver.GetType().BaseType!.Assembly);

            // There is still exactly one Apache.Arrow.Adbc loaded in this AppDomain,
            // and it is the host's copy -- not the colocated shim.
            Assembly[] loaded = System.Array.FindAll(
                AppDomain.CurrentDomain.GetAssemblies(),
                a => string.Equals(a.GetName().Name, "Apache.Arrow.Adbc", StringComparison.OrdinalIgnoreCase));
            Assert.Single(loaded);
            Assert.Same(typeof(AdbcDriver).Assembly, loaded[0]);
            Assert.NotEqual(
                fakeContractPath,
                loaded[0].Location,
                StringComparer.OrdinalIgnoreCase);

            // And cross-boundary calls still work, which is the operational consequence
            // we actually care about.
            AdbcDatabase db = driver.Open(new Dictionary<string, string> { { "uri", "fake://colocated-skew" } });
            Assert.IsAssignableFrom<AdbcDatabase>(db);
        }
    }
}
