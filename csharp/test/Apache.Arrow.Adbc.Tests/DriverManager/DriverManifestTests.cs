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
    /// Tests for <see cref="DriverManifest"/>. These cover the format documented
    /// in <c>docs/source/format/driver_manifests.rst</c>: the
    /// <c>manifest_version</c>, the optional metadata fields, and the two
    /// shapes of <c>[Driver.shared]</c> (single string vs. platform table).
    /// Also includes a real-driver round-trip against DuckDB that originally
    /// reproduced https://github.com/apache/arrow-adbc/issues/4329.
    /// </summary>
    [Collection(DriverManagerSecurityCollection.Name)]
    public class DriverManifestTests : IDisposable
    {
        private readonly List<string> _tempDirs = new List<string>();

        public void Dispose()
        {
            foreach (string d in _tempDirs)
            {
                try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
            }
        }

        // -----------------------------------------------------------------------
        // [Driver.shared] in single-string form
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_DriverSharedAsString_ReturnsThatPath()
        {
            const string toml = @"
manifest_version = 1
name = ""Example""

[Driver]
shared = ""/usr/local/lib/libadbc_driver_example.so""
";
            DriverManifest manifest = DriverManifest.LoadFromContent(toml);
            Assert.Equal("/usr/local/lib/libadbc_driver_example.so", manifest.LibraryPath);
            Assert.Equal("Example", manifest.Name);
            Assert.Equal(1, manifest.ManifestVersion);
            Assert.Null(manifest.Entrypoint);
        }

        // -----------------------------------------------------------------------
        // [Driver.shared] as a platform-tuple table
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_DriverSharedAsPlatformTable_PicksCurrentPlatform()
        {
            string current = DriverManifest.GetCurrentPlatformTuple();
            string toml =
                "manifest_version = 1\n" +
                "\n[Driver.shared]\n" +
                current + " = \"/path/for/this/platform\"\n" +
                "irrelevant_platform = \"/should/not/match\"\n";

            DriverManifest manifest = DriverManifest.LoadFromContent(toml);
            Assert.Equal("/path/for/this/platform", manifest.LibraryPath);
        }

        [Fact]
        public void LoadFromContent_NoMatchingPlatform_ThrowsNotFound()
        {
            // Build a table that intentionally excludes the current platform.
            const string toml = @"
manifest_version = 1

[Driver.shared]
made_up_os_made_up_arch = ""/nowhere""
";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManifest.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
            // Error message should mention the current platform tuple
            Assert.Contains(DriverManifest.GetCurrentPlatformTuple(), ex.Message);
        }

        // -----------------------------------------------------------------------
        // Metadata fields
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_ReadsAllMetadataFields()
        {
            // Mixes single- and double-quoted strings to exercise both forms.
            const string toml = @"
manifest_version = 1

name = ""Display Name""
version = '1.5.2'
publisher = 'ExampleCo'
license = 'Apache-2.0'
source = 'pkg-manager'

[Driver]
entrypoint = ""AdbcDriverExampleInit""
shared = ""/path/lib.so""
";
            DriverManifest manifest = DriverManifest.LoadFromContent(toml);
            Assert.Equal("Display Name", manifest.Name);
            Assert.Equal("1.5.2", manifest.Version);
            Assert.Equal("ExampleCo", manifest.Publisher);
            Assert.Equal("Apache-2.0", manifest.License);
            Assert.Equal("pkg-manager", manifest.Source);
            Assert.Equal("AdbcDriverExampleInit", manifest.Entrypoint);
        }

        // -----------------------------------------------------------------------
        // manifest_version defaulting + validation
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_NoManifestVersion_DefaultsTo1()
        {
            const string toml = @"
[Driver]
shared = ""/path/lib.so""
";
            DriverManifest manifest = DriverManifest.LoadFromContent(toml);
            Assert.Equal(1, manifest.ManifestVersion);
        }

        [Fact]
        public void LoadFromContent_ManifestVersion2_ThrowsNotImplemented()
        {
            const string toml = @"
manifest_version = 2

[Driver]
shared = ""/path/lib.so""
";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManifest.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
        }

        [Fact]
        public void LoadFromContent_ManifestVersionAsString_ThrowsInvalidArgument()
        {
            // Despite the docs example showing 'version = "1.5.2"' (which is the
            // driver's own version), manifest_version itself must be an integer.
            const string toml = @"
manifest_version = ""1""

[Driver]
shared = ""/path/lib.so""
";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManifest.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Missing Driver.shared
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_NoLibraryPath_ThrowsInvalidArgument()
        {
            const string toml = @"
manifest_version = 1
name = ""Example""

[Driver]
entrypoint = ""AdbcDriverInit""
";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManifest.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void LoadFromContent_EmptyDriverSharedString_ThrowsInvalidArgument()
        {
            const string toml = @"
manifest_version = 1

[Driver]
shared = """"
";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManifest.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Malformed TOML wraps to AdbcException
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_MalformedToml_ThrowsAdbcException()
        {
            // Unterminated string -> FormatException out of TomlParser, wrapped.
            const string toml = "manifest_version = 1\n[Driver]\nshared = \"unterminated\n";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManifest.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Round-trip with the literal-string form used in the docs example
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadFromContent_DocsExampleStyle_Parses()
        {
            // Mirrors docs/source/cpp/recipe_driver/driver_example.toml.in
            // (which uses single-quoted strings throughout).
            string current = DriverManifest.GetCurrentPlatformTuple();
            string toml =
                "manifest_version = 1\n" +
                "\n" +
                "name = 'Driver Example'\n" +
                "publisher = 'arrow-adbc-docs'\n" +
                "license = 'Apache-2.0'\n" +
                "version = '1.0.0'\n" +
                "source = 'recipe'\n" +
                "\n" +
                "[ADBC]\n" +
                "version = 'v1.1.0'\n" +
                "\n" +
                "[Driver]\n" +
                "[Driver.shared]\n" +
                current + " = '/opt/adbc/libadbc_driver_example.so'\n";

            DriverManifest manifest = DriverManifest.LoadFromContent(toml);
            Assert.Equal("Driver Example", manifest.Name);
            Assert.Equal("1.0.0", manifest.Version);
            Assert.Equal("/opt/adbc/libadbc_driver_example.so", manifest.LibraryPath);
        }

        // -----------------------------------------------------------------------
        // End-to-end against a real driver (DuckDB). Originally written as the
        // regression test for https://github.com/apache/arrow-adbc/issues/4329:
        // FindLoadDriver was routing real driver manifests through the
        // connection-profile loader, which rejected the spec-correct
        // `version = "1.5.2"` field as a non-integer.
        // -----------------------------------------------------------------------

        [Fact]
        public void FindLoadDriver_WithRealDriverManifest_LoadsDuckDbDriver()
        {
            // Locate the DuckDB native library copied next to the test assembly
            // by the CopyDuckDb MSBuild target (see Apache.Arrow.Adbc.Testing.csproj).
            string root = Directory.GetCurrentDirectory();
            string duckdbFile;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                duckdbFile = Path.Combine(root, "duckdb.dll");
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                duckdbFile = Path.Combine(root, "libduckdb.dylib");
            else
                duckdbFile = Path.Combine(root, "libduckdb.so");

            Assert.True(File.Exists(duckdbFile), $"DuckDB library missing at {duckdbFile}");

            string current = DriverManifest.GetCurrentPlatformTuple();
            // TOML basic strings interpret \ as an escape, so backslashes in
            // Windows paths must be doubled.
            string tomlEscapedPath = duckdbFile.Replace("\\", "\\\\");

            string manifest =
                "manifest_version = 1\n" +
                "\n" +
                "name = \"DuckDB\"\n" +
                "version = \"1.5.2\"        # driver version - a string per the spec\n" +
                "publisher = \"duckdb.org\"\n" +
                "license = \"MIT\"\n" +
                "\n" +
                "[ADBC]\n" +
                "version = \"1.1.0\"\n" +
                "\n" +
                "[Driver]\n" +
                "entrypoint = \"duckdb_adbc_init\"\n" +
                "\n" +
                "[Driver.shared]\n" +
                current + " = \"" + tomlEscapedPath + "\"\n";

            string tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            _tempDirs.Add(tempDir);

            string manifestPath = Path.Combine(tempDir, "duckdb.toml");
            File.WriteAllText(manifestPath, manifest);

            using AdbcDriver driver = AdbcDriverManager.FindLoadDriver(
                "duckdb",
                entrypoint: "duckdb_adbc_init",
                loadOptions: AdbcLoadFlags.Default,
                additionalSearchPathList: tempDir);

            Assert.NotNull(driver);
        }
    }
}
