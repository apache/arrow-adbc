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
using Apache.Arrow.Adbc.DriverManager;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// A driver that does not exist should be reported as an
    /// <see cref="AdbcException"/> with <see cref="AdbcStatusCode.NotFound"/>
    /// no matter how it was specified or which search locations are enabled.
    /// A library file that exists but cannot be loaded is also reported as
    /// <see cref="AdbcStatusCode.NotFound"/>, with the load error attached,
    /// matching the C/C++ driver manager. A manifest file that doesn't exist
    /// is reported as <see cref="AdbcStatusCode.InvalidArgument"/>.
    /// </summary>
    [Collection(DriverManagerSecurityCollection.Name)]
    public class DriverNotFoundTests : IDisposable
    {
        private const string MissingDriverName = "adbc_test_driver_manager_missing_driver_7c8f5482";

        private readonly List<string> _tempDirs = new List<string>();

        public void Dispose()
        {
            foreach (string d in _tempDirs)
            {
                try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
            }
        }

        public static IEnumerable<object[]> AllLoadFlags()
        {
            AdbcLoadFlags all = AdbcLoadFlags.SearchEnv | AdbcLoadFlags.SearchUser |
                AdbcLoadFlags.SearchSystem | AdbcLoadFlags.AllowRelativePaths;
            for (int flags = 0; flags <= (int)all; flags++)
            {
                yield return new object[] { (AdbcLoadFlags)flags };
            }
        }

        [Theory]
        [MemberData(nameof(AllLoadFlags))]
        public void BareName_AllLoadFlags_ThrowsNotFound(AdbcLoadFlags flags)
        {
            AssertNotFound(MissingDriverName, flags);
        }

        [Theory]
        [InlineData(AdbcLoadFlags.Default)]
        [InlineData(AdbcLoadFlags.None)]
        public void BareName_AdditionalSearchPath_ThrowsNotFound(AdbcLoadFlags flags)
        {
            AssertNotFound(MissingDriverName, flags, MissingDriverDir());
        }

        [Theory]
        [InlineData(AdbcLoadFlags.Default)]
        [InlineData(AdbcLoadFlags.None)]
        public void AbsolutePathWithoutExtension_ThrowsNotFound(AdbcLoadFlags flags)
        {
            AssertNotFound(Path.Combine(MissingDriverDir(), MissingDriverName), flags);
        }

        [Theory]
        [InlineData(AdbcLoadFlags.Default)]
        [InlineData(AdbcLoadFlags.None)]
        public void AbsolutePathWithLibraryExtension_ThrowsNotFound(AdbcLoadFlags flags)
        {
            string path = Path.Combine(MissingDriverDir(), PlatformLibraryName(MissingDriverName));
            AdbcException ex = AssertNotFound(path, flags);
            Assert.Contains(path, ex.Message);
        }

        [Fact]
        public void RelativePathWithLibraryExtension_ThrowsNotFound()
        {
            AssertNotFound(PlatformLibraryName(MissingDriverName), AdbcLoadFlags.Compatible);
        }

        // A manifest file that doesn't exist is InvalidArgument rather than
        // NotFound, as in the C/C++ and Rust driver managers
        [Theory]
        [InlineData(AdbcLoadFlags.Default)]
        [InlineData(AdbcLoadFlags.None)]
        public void AbsoluteManifestPath_ThrowsInvalidArgument(AdbcLoadFlags flags)
        {
            string path = Path.Combine(MissingDriverDir(), MissingDriverName + ".toml");
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.FindLoadDriver(path, loadOptions: flags));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Contains(path, ex.Message);
        }

        [Fact]
        public void RelativeManifestPath_ThrowsInvalidArgument()
        {
            string name = MissingDriverName + ".toml";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.FindLoadDriver(name, loadOptions: AdbcLoadFlags.Compatible));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Contains(name, ex.Message);
        }

        [Fact]
        public void ManifestWithMissingLibrary_ThrowsNotFound()
        {
            string dir = CreateTempDir();
            string libraryPath = Path.Combine(dir, PlatformLibraryName(MissingDriverName));
            string manifestPath = WriteManifest(dir, libraryPath);

            AdbcException ex = AssertNotFound(manifestPath, AdbcLoadFlags.Default);
            Assert.Contains(libraryPath, ex.Message);
        }

        // -----------------------------------------------------------------------
        // A library file that exists but is not a loadable shared library
        // -----------------------------------------------------------------------

        [Fact]
        public void InvalidLibrary_AbsolutePath_ThrowsNotFound()
        {
            string libraryPath = WriteInvalidLibrary(CreateTempDir(), PlatformLibraryName(MissingDriverName));

            AdbcException ex = AssertNotFound(libraryPath, AdbcLoadFlags.Default);
            Assert.Contains(libraryPath, ex.Message);
            Assert.NotNull(ex.InnerException);
        }

        [Fact]
        public void InvalidLibrary_BareNameInSearchPath_ThrowsNotFound()
        {
            string dir = CreateTempDir();
            string extension = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ".dll"
                : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? ".dylib"
                : ".so";
            string libraryPath = WriteInvalidLibrary(dir, MissingDriverName + extension);

            AdbcException ex = AssertNotFound(MissingDriverName, AdbcLoadFlags.None, dir);
            Assert.Contains(libraryPath, ex.Message);
            Assert.NotNull(ex.InnerException);
        }

        [Fact]
        public void ManifestWithInvalidLibrary_ThrowsNotFound()
        {
            string dir = CreateTempDir();
            string libraryPath = WriteInvalidLibrary(dir, PlatformLibraryName(MissingDriverName));
            string manifestPath = WriteManifest(dir, libraryPath);

            AdbcException ex = AssertNotFound(manifestPath, AdbcLoadFlags.Default);
            Assert.Contains(libraryPath, ex.Message);
            Assert.NotNull(ex.InnerException);
        }

        private static AdbcException AssertNotFound(string driverName, AdbcLoadFlags flags, string? additionalSearchPathList = null)
        {
            Exception? ex = Record.Exception(
                () => AdbcDriverManager.FindLoadDriver(
                    driverName,
                    loadOptions: flags,
                    additionalSearchPathList: additionalSearchPathList));

            Assert.True(
                ex is AdbcException,
                $"Expected AdbcException(NotFound), got {ex?.GetType().FullName ?? "no exception"}: {ex?.Message}");
            AdbcException adbcEx = (AdbcException)ex!;
            Assert.Equal(AdbcStatusCode.NotFound, adbcEx.Status);
            return adbcEx;
        }

        private string CreateTempDir()
        {
            string dir = Path.Combine(Path.GetTempPath(), "adbc_driver_not_found_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            _tempDirs.Add(dir);
            return dir;
        }

        private static string WriteInvalidLibrary(string dir, string fileName)
        {
            string path = Path.Combine(dir, fileName);
            File.WriteAllText(path, "this is not a shared library");
            return path;
        }

        private static string WriteManifest(string dir, string libraryPath)
        {
            string path = Path.Combine(dir, MissingDriverName + ".toml");
            File.WriteAllText(path,
                "manifest_version = 1\n" +
                "name = \"Missing\"\n" +
                "\n[Driver]\n" +
                "shared = '" + libraryPath + "'\n");
            return path;
        }

        private static string MissingDriverDir() =>
            Path.Combine(Path.GetTempPath(), "adbc_missing_driver_dir_7c8f5482");

        private static string PlatformLibraryName(string name)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return name + ".dll";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return "lib" + name + ".dylib";
            return "lib" + name + ".so";
        }
    }
}
