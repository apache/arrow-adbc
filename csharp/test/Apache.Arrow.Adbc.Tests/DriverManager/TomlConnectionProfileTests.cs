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
    /// Tests for <see cref="FilesystemProfileProvider"/> TOML parsing and the internal <see cref="TomlParser"/>.
    /// </summary>
    /// <remarks>
    /// Some tests in this class load drivers via <c>AdbcDriverManager</c>, whose
    /// hot path reads the process-wide <c>DriverManagerSecurity.Allowlist</c> and
    /// fans every load attempt into <c>DriverManagerSecurity.AuditLogger</c>.
    /// Joining the <see cref="DriverManagerSecurityCollection"/> serializes us
    /// against tests that mutate those static values, preventing flaky
    /// "collection was modified" / "not permitted by the configured allowlist"
    /// failures.
    /// </remarks>
    [Collection(DriverManagerSecurityCollection.Name)]
    public class TomlConnectionProfileTests : IDisposable
    {
        // -----------------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------------

        private readonly List<string> _tempFiles = new List<string>();

        private string WriteTempToml(string content)
        {
            string path = Path.GetTempFileName();
            // Rename so it has the .toml extension (required by load helpers).
            string tomlPath = Path.ChangeExtension(path, ".toml");
            File.Move(path, tomlPath);
            File.WriteAllText(tomlPath, content, System.Text.Encoding.UTF8);
            _tempFiles.Add(tomlPath);
            return tomlPath;
        }

        public void Dispose()
        {
            foreach (string f in _tempFiles)
            {
                try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
            }
        }

        // -----------------------------------------------------------------------
        // Positive: basic profile with all option types
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_WithAllOptionTypes_SetsCorrectValues()
        {
            const string toml = @"
version = 1
driver = ""libadbc_driver_postgresql""

[options]
uri = ""postgresql://localhost/mydb""
port = 5432
timeout = 30.5
use_ssl = true
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Assert.Equal("libadbc_driver_postgresql", profile.DriverName);
            Assert.Equal("postgresql://localhost/mydb", profile.StringOptions["uri"]);
            Assert.Equal(5432L, profile.IntOptions["port"]);
            Assert.Equal(30.5, profile.DoubleOptions["timeout"]);
            // Booleans are converted to string
            Assert.Equal("true", profile.StringOptions["use_ssl"]);
        }

        // -----------------------------------------------------------------------
        // New spec format: profile_version and [Options]
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_NewFormat_WithProfileVersionAndOptions_SetsCorrectValues()
        {
            const string toml = @"
profile_version = 1
driver = ""libadbc_driver_postgresql""

[Options]
uri = ""postgresql://localhost/mydb""
port = 5432
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Assert.Equal("libadbc_driver_postgresql", profile.DriverName);
            Assert.Equal("postgresql://localhost/mydb", profile.StringOptions["uri"]);
            Assert.Equal(5432L, profile.IntOptions["port"]);
        }

        [Fact]
        public void ParseProfile_LegacyFormat_WithVersionAndLowercaseOptions_SetsCorrectValues()
        {
            // Legacy format should still work for backward compatibility
            const string toml = @"
version = 1
driver = ""mydriver""

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Assert.Equal("mydriver", profile.DriverName);
            Assert.Equal("value", profile.StringOptions["key"]);
        }

        [Fact]
        public void ParseProfile_MixedFormat_ProfileVersionWithLowercaseOptions_Works()
        {
            // Mix of new and old should work
            const string toml = @"
profile_version = 1
driver = ""mydriver""

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Assert.Equal("mydriver", profile.DriverName);
            Assert.Equal("value", profile.StringOptions["key"]);
        }

        [Fact]
        public void ParseProfile_OptionsSectionIsCaseInsensitive()
        {
            // The TOML parser treats section names case-insensitively
            // so [Options], [options], [OPTIONS] all work the same
            const string toml = @"
profile_version = 1
driver = ""mydriver""

[OPTIONS]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Assert.Equal("value", profile.StringOptions["key"]);
        }

        [Fact]
        public void ParseProfile_WithFalseBoolean_SetsStringFalse()
        {
            const string toml = @"
version = 1
driver = ""mydriver""

[options]
verify_cert = false
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal("false", profile.StringOptions["verify_cert"]);
        }

        [Fact]
        public void ParseProfile_WithoutDriver_DriverNameIsNull()
        {
            const string toml = @"
version = 1

[options]
uri = ""jdbc:something""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Null(profile.DriverName);
            Assert.Equal("jdbc:something", profile.StringOptions["uri"]);
        }

        [Fact]
        public void ParseProfile_WithoutOptionsSection_OptionDictionariesAreEmpty()
        {
            const string toml = @"
version = 1
driver = ""mydriver""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal("mydriver", profile.DriverName);
            Assert.Empty(profile.StringOptions);
            Assert.Empty(profile.IntOptions);
            Assert.Empty(profile.DoubleOptions);
        }

        [Fact]
        public void ParseProfile_WithLineComments_CommentsAreIgnored()
        {
            const string toml = @"
# This is a comment
version = 1
driver = ""mydriver"" # inline comment

[options]
# another comment
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal("mydriver", profile.DriverName);
            Assert.Equal("value", profile.StringOptions["key"]);
        }

        [Fact]
        public void ParseProfile_WithNegativeInteger_ParsedCorrectly()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
offset = -100
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal(-100L, profile.IntOptions["offset"]);
        }

        [Fact]
        public void ParseProfile_WithNegativeDouble_ParsedCorrectly()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
delta = -0.5
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal(-0.5, profile.DoubleOptions["delta"]);
        }

        [Fact]
        public void ParseProfile_FromFile_LoadsCorrectly()
        {
            const string toml = @"
version = 1
driver = ""file_driver""

[options]
server = ""localhost""
";
            string path = WriteTempToml(toml);
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromFile(path);

            Assert.Equal("file_driver", profile.DriverName);
            Assert.Equal("localhost", profile.StringOptions["server"]);
        }

        [Fact]
        public void ParseProfile_EscapedQuotesInString_ParsedCorrectly()
        {
            const string toml = "version = 1\ndriver = \"d\"\n\n[options]\nmsg = \"say \\\"hello\\\"\"\n";

            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal("say \"hello\"", profile.StringOptions["msg"]);
        }

        // -----------------------------------------------------------------------
        // Positive: env_var expansion
        // -----------------------------------------------------------------------

        [Fact]
        public void ResolveEnvVars_ExpandsEnvVarValues()
        {
            const string varName = "ADBC_TEST_PASSWORD_TOML";
            Environment.SetEnvironmentVariable(varName, "secret123");
            try
            {
                const string toml = @"
version = 1
driver = ""d""

[options]
password = ""{{ env_var(ADBC_TEST_PASSWORD_TOML) }}""
plain = ""notanenvvar""
";
                ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();

                Assert.Equal("secret123", profile.StringOptions["password"]);
                Assert.Equal("notanenvvar", profile.StringOptions["plain"]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(varName, null);
            }
        }

        [Fact]
        public void ResolveEnvVars_NoPlaceholders_ReturnsSameValues()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
            Assert.Equal("value", profile.StringOptions["key"]);
        }

        [Fact]
        public void ResolveEnvVars_PlaceholderEmbeddedInString_ExpandedInPlace()
        {
            // Per spec: placeholders may appear anywhere inside a value.
            const string varName = "ADBC_TEST_EMBEDDED_HOST";
            Environment.SetEnvironmentVariable(varName, "prod.example.com");
            try
            {
                const string toml = @"
version = 1
driver = ""d""

[options]
uri = ""postgres://user@{{ env_var(ADBC_TEST_EMBEDDED_HOST) }}:5432/db""
";
                ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
                Assert.Equal("postgres://user@prod.example.com:5432/db", profile.StringOptions["uri"]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(varName, null);
            }
        }

        [Fact]
        public void ResolveEnvVars_MultiplePlaceholdersInOneValue_AllExpanded()
        {
            const string hostVar = "ADBC_TEST_MULTI_HOST";
            const string portVar = "ADBC_TEST_MULTI_PORT";
            Environment.SetEnvironmentVariable(hostVar, "db.local");
            Environment.SetEnvironmentVariable(portVar, "5433");
            try
            {
                const string toml = @"
version = 1
driver = ""d""

[options]
uri = ""postgres://{{ env_var(ADBC_TEST_MULTI_HOST) }}:{{ env_var(ADBC_TEST_MULTI_PORT) }}/db""
";
                ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
                Assert.Equal("postgres://db.local:5433/db", profile.StringOptions["uri"]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(hostVar, null);
                Environment.SetEnvironmentVariable(portVar, null);
            }
        }

        [Fact]
        public void ResolveEnvVars_WhitespaceVariationsInsidePlaceholder_AllAccepted()
        {
            const string varName = "ADBC_TEST_WS_VAR";
            Environment.SetEnvironmentVariable(varName, "X");
            try
            {
                // No whitespace, lots of whitespace, asymmetric whitespace -- all valid.
                const string toml = @"
version = 1
driver = ""d""

[options]
tight = ""{{env_var(ADBC_TEST_WS_VAR)}}""
loose = ""{{    env_var(ADBC_TEST_WS_VAR)    }}""
asymmetric = ""{{ env_var(ADBC_TEST_WS_VAR)}}""
";
                ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
                Assert.Equal("X", profile.StringOptions["tight"]);
                Assert.Equal("X", profile.StringOptions["loose"]);
                Assert.Equal("X", profile.StringOptions["asymmetric"]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(varName, null);
            }
        }

        [Fact]
        public void ResolveEnvVars_BareEnvVarSyntax_NotInterpretedAsPlaceholder()
        {
            // The old (pre-spec) C# implementation treated a whole-value 'env_var(NAME)'
            // as a placeholder. The spec requires '{{ }}' delimiters, so the bare form
            // is now a literal string.
            const string toml = @"
version = 1
driver = ""d""

[options]
literal = ""env_var(NOT_A_PLACEHOLDER)""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
            Assert.Equal("env_var(NOT_A_PLACEHOLDER)", profile.StringOptions["literal"]);
        }

        // -----------------------------------------------------------------------
        // Positive: AdbcDriverManager DeriveEntrypoint
        // -----------------------------------------------------------------------

        [Theory]
        [InlineData("libadbc_driver_postgresql.so", "AdbcDriverPostgresqlInit")]
        [InlineData("libadbc_driver_snowflake.dll", "AdbcDriverSnowflakeInit")]
        [InlineData("libadbc_driver_flightsql.dylib", "AdbcDriverFlightsqlInit")]
        [InlineData("adbc_driver_sqlite.dll", "AdbcDriverSqliteInit")]
        [InlineData("mydriver.dll", "AdbcDriverMydriverInit")]
        [InlineData("unknown", "AdbcDriverUnknownInit")]
        public void DeriveEntrypoint_ReturnsExpectedSymbol(string driverPath, string expected)
        {
            string actual = AdbcDriverManager.DeriveEntrypoint(driverPath);
            Assert.Equal(expected, actual);
        }

        // -----------------------------------------------------------------------
        // Positive: AdbcLoadFlags values
        // -----------------------------------------------------------------------

        [Fact]
        public void AdbcLoadFlags_DefaultIncludesAllSearchFlags()
        {
            AdbcLoadFlags flags = AdbcLoadFlags.Default;
            Assert.True(flags.HasFlag(AdbcLoadFlags.SearchEnv));
            Assert.True(flags.HasFlag(AdbcLoadFlags.SearchUser));
            Assert.True(flags.HasFlag(AdbcLoadFlags.SearchSystem));
            // Default is hardened relative to ADBC_LOAD_FLAG_DEFAULT in the C header:
            // relative-path loading is opt-in via AllowRelativePaths or Compatible.
            Assert.False(flags.HasFlag(AdbcLoadFlags.AllowRelativePaths));
        }

        [Fact]
        public void AdbcLoadFlags_CompatibleMirrorsCHeaderDefault()
        {
            AdbcLoadFlags flags = AdbcLoadFlags.Compatible;
            Assert.True(flags.HasFlag(AdbcLoadFlags.SearchEnv));
            Assert.True(flags.HasFlag(AdbcLoadFlags.SearchUser));
            Assert.True(flags.HasFlag(AdbcLoadFlags.SearchSystem));
            Assert.True(flags.HasFlag(AdbcLoadFlags.AllowRelativePaths));
        }

        [Fact]
        public void AdbcLoadFlags_NoneHasNoFlags()
        {
            AdbcLoadFlags flags = AdbcLoadFlags.None;
            Assert.False(flags.HasFlag(AdbcLoadFlags.SearchEnv));
            Assert.False(flags.HasFlag(AdbcLoadFlags.SearchUser));
            Assert.False(flags.HasFlag(AdbcLoadFlags.SearchSystem));
            Assert.False(flags.HasFlag(AdbcLoadFlags.AllowRelativePaths));
        }


        // -----------------------------------------------------------------------
        // Negative: missing version field
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_MissingVersion_ThrowsAdbcException()
        {
            const string toml = @"
driver = ""mydriver""

[options]
key = ""value""
";
            AdbcException ex = Assert.Throws<AdbcException>(() => FilesystemProfileProvider.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Contains("profile_version", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        // -----------------------------------------------------------------------
        // Negative: unsupported version
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_UnsupportedVersion_ThrowsAdbcException()
        {
            const string toml = @"
profile_version = 99
driver = ""mydriver""
";
            AdbcException ex = Assert.Throws<AdbcException>(() => FilesystemProfileProvider.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
            Assert.Contains("99", ex.Message);
        }

        // -----------------------------------------------------------------------
        // Negative: null / empty content
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_NullContent_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => FilesystemProfileProvider.LoadFromContent(null!));
        }

        [Fact]
        public void ParseProfile_EmptyContent_ThrowsAdbcException()
        {
            AdbcException ex = Assert.Throws<AdbcException>(() => FilesystemProfileProvider.LoadFromContent(string.Empty));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Negative: file not found
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_FileNotFound_ThrowsIOException()
        {
            string fakePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".toml");
            Assert.ThrowsAny<IOException>(
                () => FilesystemProfileProvider.LoadFromFile(fakePath));
        }

        // -----------------------------------------------------------------------
        // env_var expansion – variable not set
        // -----------------------------------------------------------------------

        [Fact]
        public void ResolveEnvVars_MissingEnvVar_ExpandsToEmptyString()
        {
            // Per spec (and matching the C/C++ driver manager): a missing env var
            // expands to "" and processing of the rest of the value continues.
            // Example from the spec: "foo{{ env_var(MISSING) }}bar" -> "foobar".
            const string varName = "ADBC_TEST_DEFINITELY_NOT_SET_XYZ";
            Environment.SetEnvironmentVariable(varName, null);

            const string toml = @"
version = 1
driver = ""d""

[options]
password = ""{{ env_var(ADBC_TEST_DEFINITELY_NOT_SET_XYZ) }}""
greeting = ""foo{{ env_var(ADBC_TEST_DEFINITELY_NOT_SET_XYZ) }}bar""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
            Assert.Equal("", profile.StringOptions["password"]);
            Assert.Equal("foobar", profile.StringOptions["greeting"]);
        }

        // -----------------------------------------------------------------------
        // Negative: malformed / unsupported placeholders
        // -----------------------------------------------------------------------

        [Fact]
        public void ResolveEnvVars_UnsupportedFunction_ThrowsInvalidArgument()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
weird = ""{{ unknown_func(FOO) }}""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcException ex = Assert.Throws<AdbcException>(() => profile.ResolveEnvVars());
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void ResolveEnvVars_MissingClosingParen_ThrowsInvalidArgument()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
oops = ""{{ env_var(FOO }}""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcException ex = Assert.Throws<AdbcException>(() => profile.ResolveEnvVars());
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void ResolveEnvVars_EmptyVarName_ThrowsInvalidArgument()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
oops = ""{{ env_var() }}""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcException ex = Assert.Throws<AdbcException>(() => profile.ResolveEnvVars());
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Negative: AdbcDriverManager argument validation
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadDriver_NullPath_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.LoadDriver(null!));
        }

        [Fact]
        public void LoadDriver_EmptyPath_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.LoadDriver(string.Empty));
        }

        [Fact]
        public void FindLoadDriver_NullName_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.FindLoadDriver(null!));
        }

        [Fact]
        public void FindLoadDriver_EmptyName_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.FindLoadDriver(string.Empty));
        }

        [Fact]
        public void FindLoadDriver_NonExistentBareName_ThrowsAdbcException()
        {
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.FindLoadDriver(
                    "driver_that_does_not_exist_anywhere",
                    loadOptions: AdbcLoadFlags.None));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        [Fact]
        public void FindLoadDriver_AbsolutePathNotFound_ThrowsException()
        {
            // Absolute non-toml path that does not exist.
            string fakePath = Path.Combine(Path.GetTempPath(), "fake_adbc_driver_notexist.dll");
            Assert.ThrowsAny<Exception>(() => AdbcDriverManager.FindLoadDriver(fakePath));
        }

        [Fact]
        public void FindLoadDriver_AbsoluteTomlPathNotFound_ThrowsAdbcException()
        {
            string fakePath = Path.Combine(Path.GetTempPath(), "fake_manifest_notexist.toml");
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.FindLoadDriver(fakePath));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        [Fact]
        public void LoadDriverFromProfile_NullProfile_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => AdbcDriverManager.LoadDriverFromProfile(null!));
        }

        [Fact]
        public void LoadDriverFromProfile_ProfileWithNoDriver_ThrowsAdbcException()
        {
            const string toml = @"
version = 1

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadDriverFromProfile(profile));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Negative: FilesystemProfileProvider with no candidate
        // -----------------------------------------------------------------------

        [Fact]
        public void FilesystemProfileProvider_UnknownProfile_ReturnsNull()
        {
            FilesystemProfileProvider provider = new FilesystemProfileProvider();
            ConnectionProfile? result = provider.GetProfile(
                "definitely_not_a_real_profile_xyz",
                additionalSearchPathList: Path.GetTempPath());
            Assert.Null(result);
        }

        [Fact]
        public void FilesystemProfileProvider_NullName_ThrowsArgumentNullException()
        {
            FilesystemProfileProvider provider = new FilesystemProfileProvider();
            Assert.Throws<ArgumentNullException>(() => provider.GetProfile(null!));
        }

        // -----------------------------------------------------------------------
        // Positive: FilesystemProfileProvider finds a profile on disk
        // -----------------------------------------------------------------------

        [Fact]
        public void FilesystemProfileProvider_ProfileExistsInAdditionalPath_ReturnsProfile()
        {
            const string toml = @"
version = 1
driver = ""test_driver""

[options]
key = ""found""
";
            string dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            string filePath = Path.Combine(dir, "myprofile.toml");
            File.WriteAllText(filePath, toml, System.Text.Encoding.UTF8);
            try
            {
                FilesystemProfileProvider provider = new FilesystemProfileProvider();
                ConnectionProfile? profile = provider.GetProfile("myprofile", additionalSearchPathList: dir);

                Assert.NotNull(profile);
                Assert.Equal("test_driver", profile!.DriverName);
                Assert.Equal("found", profile.StringOptions["key"]);
            }
            finally
            {
                try { File.Delete(filePath); } catch { }
                try { Directory.Delete(dir); } catch { }
            }
        }

        [Fact]
        public void FilesystemProfileProvider_AbsoluteTomlPath_LoadsDirectly()
        {
            const string toml = @"
version = 1
driver = ""abs_driver""
";
            string path = WriteTempToml(toml);
            FilesystemProfileProvider provider = new FilesystemProfileProvider();
            ConnectionProfile? profile = provider.GetProfile(path);

            Assert.NotNull(profile);
            Assert.Equal("abs_driver", profile!.DriverName);
        }

        // -----------------------------------------------------------------------
        // Positive: LoadManagedDriver loads a managed .NET driver by reflection
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadManagedDriver_ValidType_ReturnsDriverInstance()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(assemblyPath, typeName);

            Assert.NotNull(driver);
            // Use type name comparison to avoid assembly identity issues when loaded via Assembly.LoadFrom
            Assert.Equal(typeName, driver.GetType().FullName);
        }

        // -----------------------------------------------------------------------
        // Positive: BuildStringOptions merges all option types into string dictionary
        // -----------------------------------------------------------------------

        [Fact]
        public void BuildStringOptions_MergesAllOptionTypes()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
str_key = ""hello""
int_key = 42
dbl_key = 1.5
bool_key = true
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            IReadOnlyDictionary<string, string> opts = AdbcDriverManager.BuildStringOptions(profile);

            Assert.Equal("hello", opts["str_key"]);
            Assert.Equal("42", opts["int_key"]);
            Assert.Equal("1.5", opts["dbl_key"]);
            Assert.Equal("true", opts["bool_key"]);
        }

        [Fact]
        public void BuildStringOptions_NoOptions_ReturnsEmptyDictionary()
        {
            const string toml = @"
version = 1
driver = ""d""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            IReadOnlyDictionary<string, string> opts = AdbcDriverManager.BuildStringOptions(profile);

            Assert.Empty(opts);
        }

        // -----------------------------------------------------------------------
        // Positive: OpenDatabaseFromProfile end-to-end with managed driver
        //
        // Managed drivers are selected by a scheme-prefixed 'entrypoint' option:
        // dotnet:Type for modern .NET, netfx:Type for .NET Framework. The driver
        // manager consumes the entrypoint option before opening the database.
        // -----------------------------------------------------------------------

        /// <summary>
        /// Scheme prefix this test process must use when selecting managed drivers
        /// -- a dotnet: entrypoint on .NET Framework (or vice versa) is a runtime
        /// mismatch that the driver manager intentionally rejects.
        /// </summary>
        private static string ManagedScheme =>
#if NETFRAMEWORK
            "netfx:";
#else
            "dotnet:";
#endif

        [Fact]
        public void OpenDatabaseFromProfile_ManagedDriver_OpensDatabase()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "profile_version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "\n[Options]\n"
                + "entrypoint = \"" + ManagedScheme + typeName + "\"\n"
                + "project_id = \"my-project\"\n"
                + "region = \"us-east1\"\n";

            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile);

            Assert.Equal("Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDatabase", db.GetType().FullName);

            System.Reflection.PropertyInfo? paramsProp = db.GetType().GetProperty("Parameters");
            Assert.NotNull(paramsProp);
            IReadOnlyDictionary<string, string> parameters = (IReadOnlyDictionary<string, string>)paramsProp!.GetValue(db)!;
            Assert.Equal("my-project", parameters["project_id"]);
            Assert.Equal("us-east1", parameters["region"]);
            // entrypoint is consumed by the driver manager, not forwarded to the driver
            Assert.False(parameters.ContainsKey("entrypoint"));
        }

        // -----------------------------------------------------------------------
        // Negative: LoadManagedDriver argument validation
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadManagedDriver_NullAssemblyPath_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.LoadManagedDriver(null!, "SomeType"));
        }

        [Fact]
        public void LoadManagedDriver_EmptyAssemblyPath_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.LoadManagedDriver(string.Empty, "SomeType"));
        }

        [Fact]
        public void LoadManagedDriver_NullTypeName_ThrowsArgumentException()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            Assert.Throws<ArgumentException>(() => AdbcDriverManager.LoadManagedDriver(assemblyPath, null!));
        }

        [Fact]
        public void LoadManagedDriver_TypeNotFound_ThrowsAdbcExceptionWithNotFoundStatus()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(assemblyPath, "NoSuchType.DoesNotExist"));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        [Fact]
        public void LoadManagedDriver_TypeNotAdbcDriver_ThrowsAdbcExceptionWithInvalidArgumentStatus()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string nonDriverTypeName = typeof(TomlConnectionProfileTests).FullName!;
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(assemblyPath, nonDriverTypeName));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Negative: OpenDatabaseFromProfile managed path validation
        // -----------------------------------------------------------------------

        [Fact]
        public void OpenDatabaseFromProfile_NullProfile_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => AdbcDriverManager.OpenDatabaseFromProfile(null!));
        }

        [Fact]
        public void OpenDatabaseFromProfile_NoDriver_ThrowsAdbcException()
        {
            // A profile with no 'driver' field cannot be opened on its own.
            const string toml = @"
profile_version = 1

[Options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.OpenDatabaseFromProfile(profile));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Negative: BuildStringOptions null validation
        // -----------------------------------------------------------------------

        [Fact]
        public void BuildStringOptions_NullProfile_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => AdbcDriverManager.BuildStringOptions(null!));
        }

        // -----------------------------------------------------------------------
        // Bad driver: LoadManagedDriver with assembly file that does not exist
        // -----------------------------------------------------------------------

        [Fact]
        public void LoadManagedDriver_NonExistentAssemblyFile_ThrowsAdbcExceptionWithNotFoundStatus()
        {
            string missingPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".dll");
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(missingPath, "Some.Type"));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Invalid types: version field is an unquoted non-numeric string
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_VersionIsNonNumericString_ThrowsAdbcException()
        {
            // The parser falls back to treating unquoted, non-bool, non-numeric tokens as
            // raw strings.  ValidateVersion must not leak a raw FormatException.
            const string toml = "version = notanumber\ndriver = \"d\"\n";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => FilesystemProfileProvider.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void ParseProfile_VersionIsQuotedNonNumericString_ThrowsAdbcException()
        {
            // A quoted string that cannot be converted to long must not leak FormatException.
            const string toml = "version = \"abc\"\ndriver = \"d\"\n";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => FilesystemProfileProvider.LoadFromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Unknown TOML sections are silently ignored
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_UnknownSection_SectionIsIgnored()
        {
            const string toml = @"
version = 1
driver = ""d""

[metadata]
author = ""someone""

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            // [metadata] must not bleed into options
            Assert.Equal("value", profile.StringOptions["key"]);
            Assert.False(profile.StringOptions.ContainsKey("author"));
        }

        // -----------------------------------------------------------------------
        // Duplicate keys: last value wins
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_DuplicateOptionKey_LastValueWins()
        {
            const string toml = "version = 1\ndriver = \"d\"\n\n[options]\nkey = \"first\"\nkey = \"second\"\n";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Assert.Equal("second", profile.StringOptions["key"]);
        }

        // -----------------------------------------------------------------------
        // ResolveEnvVars preserves the driver reference and other non-env values
        // -----------------------------------------------------------------------

        [Fact]
        public void ResolveEnvVars_DriverNameIsPreserved()
        {
            const string varName = "ADBC_TEST_RESOLVE_ENVVAR_HOST";
            Environment.SetEnvironmentVariable(varName, "myhost");
            try
            {
                const string toml = @"
version = 1
driver = ""MyDriver.dll""

[options]
host = ""{{ env_var(ADBC_TEST_RESOLVE_ENVVAR_HOST) }}""
";
                ConnectionProfile resolved = FilesystemProfileProvider.LoadFromContent(toml).ResolveEnvVars();
                Assert.Equal("MyDriver.dll", resolved.DriverName);
                Assert.Equal("myhost", resolved.StringOptions["host"]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(varName, null);
            }
        }

        // -----------------------------------------------------------------------
        // Extra options not known to the driver pass through transparently
        // -----------------------------------------------------------------------

        [Fact]
        public void OpenDatabaseFromProfile_UnknownOptionsPassThroughToDriver()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "profile_version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "\n[Options]\n"
                + "entrypoint = \"" + ManagedScheme + typeName + "\"\n"
                + "known_key = \"hello\"\n"
                + "unknown_widget = \"ignored_by_driver\"\n";

            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile);

            Assert.Equal("Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDatabase", db.GetType().FullName);

            System.Reflection.PropertyInfo? paramsProp = db.GetType().GetProperty("Parameters");
            Assert.NotNull(paramsProp);
            IReadOnlyDictionary<string, string> parameters = (IReadOnlyDictionary<string, string>)paramsProp!.GetValue(db)!;

            // Both keys arrive at the driver; the driver decides what to do with them.
            Assert.Equal("hello", parameters["known_key"]);
            Assert.Equal("ignored_by_driver", parameters["unknown_widget"]);
        }

        // -----------------------------------------------------------------------
        // Positive: BuildStringOptions with explicit options merges correctly
        // -----------------------------------------------------------------------

        [Fact]
        public void BuildStringOptions_WithExplicitOptions_MergesCorrectly()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
profile_key = ""from_profile""
shared_key = ""profile_value""
port = 5432
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Dictionary<string, string> explicitOptions = new Dictionary<string, string>
            {
                { "explicit_key", "from_explicit" },
                { "shared_key", "explicit_value" }
            };

            IReadOnlyDictionary<string, string> merged = AdbcDriverManager.BuildStringOptions(profile, explicitOptions);

            // Profile-only option should be present
            Assert.Equal("from_profile", merged["profile_key"]);
            Assert.Equal("5432", merged["port"]);

            // Explicit-only option should be present
            Assert.Equal("from_explicit", merged["explicit_key"]);

            // Shared key: explicit should override profile
            Assert.Equal("explicit_value", merged["shared_key"]);
        }

        [Fact]
        public void BuildStringOptions_NullExplicitOptions_UsesOnlyProfileOptions()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            IReadOnlyDictionary<string, string> opts = AdbcDriverManager.BuildStringOptions(profile, null);

            Assert.Equal("value", opts["key"]);
        }

        [Fact]
        public void BuildStringOptions_EmptyExplicitOptions_UsesOnlyProfileOptions()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
key = ""value""
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            Dictionary<string, string> empty = new Dictionary<string, string>();
            IReadOnlyDictionary<string, string> opts = AdbcDriverManager.BuildStringOptions(profile, empty);

            Assert.Equal("value", opts["key"]);
        }

        [Fact]
        public void BuildStringOptions_ExplicitOptionsOverrideAllProfileTypes()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
str_key = ""profile_string""
int_key = 100
dbl_key = 2.5
bool_key = false
";
            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Dictionary<string, string> explicitOptions = new Dictionary<string, string>
            {
                { "str_key", "explicit_string" },
                { "int_key", "999" },
                { "dbl_key", "9.9" },
                { "bool_key", "true" }
            };

            IReadOnlyDictionary<string, string> merged = AdbcDriverManager.BuildStringOptions(profile, explicitOptions);

            // All explicit values should override profile values
            Assert.Equal("explicit_string", merged["str_key"]);
            Assert.Equal("999", merged["int_key"]);
            Assert.Equal("9.9", merged["dbl_key"]);
            Assert.Equal("true", merged["bool_key"]);
        }

        // -----------------------------------------------------------------------
        // Positive: OpenDatabaseFromProfile with explicit options
        // -----------------------------------------------------------------------

        [Fact]
        public void OpenDatabaseFromProfile_WithExplicitOptions_MergesCorrectly()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "profile_version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "\n[Options]\n"
                + "entrypoint = \"" + ManagedScheme + typeName + "\"\n"
                + "profile_option = \"from_profile\"\n"
                + "shared_option = \"profile_value\"\n";

            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);

            Dictionary<string, string> explicitOptions = new Dictionary<string, string>
            {
                { "explicit_option", "from_explicit" },
                { "shared_option", "explicit_value" }
            };

            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile, explicitOptions);

            Assert.Equal("Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDatabase", db.GetType().FullName);

            System.Reflection.PropertyInfo? paramsProp = db.GetType().GetProperty("Parameters");
            Assert.NotNull(paramsProp);
            IReadOnlyDictionary<string, string> parameters = (IReadOnlyDictionary<string, string>)paramsProp!.GetValue(db)!;

            Assert.Equal("from_profile", parameters["profile_option"]);
            Assert.Equal("from_explicit", parameters["explicit_option"]);
            Assert.Equal("explicit_value", parameters["shared_option"]);
        }

        [Fact]
        public void OpenDatabaseFromProfile_NullExplicitOptions_UsesOnlyProfile()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "profile_version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "\n[Options]\n"
                + "entrypoint = \"" + ManagedScheme + typeName + "\"\n"
                + "key = \"value\"\n";

            ConnectionProfile profile = FilesystemProfileProvider.LoadFromContent(toml);
            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile, null);

            Assert.Equal("Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDatabase", db.GetType().FullName);

            System.Reflection.PropertyInfo? paramsProp = db.GetType().GetProperty("Parameters");
            Assert.NotNull(paramsProp);
            IReadOnlyDictionary<string, string> parameters = (IReadOnlyDictionary<string, string>)paramsProp!.GetValue(db)!;
            Assert.Equal("value", parameters["key"]);
        }
    }
}
