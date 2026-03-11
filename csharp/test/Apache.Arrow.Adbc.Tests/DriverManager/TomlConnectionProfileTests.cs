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
    /// Tests for <see cref="TomlConnectionProfile"/> and the internal <see cref="TomlParser"/>.
    /// </summary>
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);

            Assert.Equal("libadbc_driver_postgresql", profile.DriverName);
            Assert.Equal("postgresql://localhost/mydb", profile.StringOptions["uri"]);
            Assert.Equal(5432L, profile.IntOptions["port"]);
            Assert.Equal(30.5, profile.DoubleOptions["timeout"]);
            // Booleans are converted to string
            Assert.Equal("true", profile.StringOptions["use_ssl"]);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromFile(path);

            Assert.Equal("file_driver", profile.DriverName);
            Assert.Equal("localhost", profile.StringOptions["server"]);
        }

        [Fact]
        public void ParseProfile_EscapedQuotesInString_ParsedCorrectly()
        {
            const string toml = "version = 1\ndriver = \"d\"\n\n[options]\nmsg = \"say \\\"hello\\\"\"\n";

            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
password = ""env_var(ADBC_TEST_PASSWORD_TOML)""
plain = ""notanenvvar""
";
                TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml).ResolveEnvVars();

                Assert.Equal("secret123", profile.StringOptions["password"]);
                Assert.Equal("notanenvvar", profile.StringOptions["plain"]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(varName, null);
            }
        }

        [Fact]
        public void ResolveEnvVars_NoEnvVarValues_ReturnsSameProfile()
        {
            const string toml = @"
version = 1
driver = ""d""

[options]
key = ""value""
";
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml).ResolveEnvVars();
            Assert.Equal("value", profile.StringOptions["key"]);
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
            AdbcException ex = Assert.Throws<AdbcException>(() => TomlConnectionProfile.FromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Contains("version", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        // -----------------------------------------------------------------------
        // Negative: unsupported version
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_UnsupportedVersion_ThrowsAdbcException()
        {
            const string toml = @"
version = 99
driver = ""mydriver""
";
            AdbcException ex = Assert.Throws<AdbcException>(() => TomlConnectionProfile.FromContent(toml));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
            Assert.Contains("99", ex.Message);
        }

        // -----------------------------------------------------------------------
        // Negative: null / empty content
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_NullContent_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => TomlConnectionProfile.FromContent(null!));
        }

        [Fact]
        public void ParseProfile_EmptyContent_ThrowsAdbcException()
        {
            AdbcException ex = Assert.Throws<AdbcException>(() => TomlConnectionProfile.FromContent(string.Empty));
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
                () => TomlConnectionProfile.FromFile(fakePath));
        }

        // -----------------------------------------------------------------------
        // Negative: env_var expansion – variable not set
        // -----------------------------------------------------------------------

        [Fact]
        public void ResolveEnvVars_MissingEnvVar_ThrowsAdbcException()
        {
            const string varName = "ADBC_TEST_DEFINITELY_NOT_SET_XYZ";
            Environment.SetEnvironmentVariable(varName, null);

            const string toml = @"
version = 1
driver = ""d""

[options]
password = ""env_var(ADBC_TEST_DEFINITELY_NOT_SET_XYZ)""
";
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            AdbcException ex = Assert.Throws<AdbcException>(() => profile.ResolveEnvVars());
            Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            Assert.Contains(varName, ex.Message);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            var provider = new FilesystemProfileProvider();
            IConnectionProfile? result = provider.GetProfile(
                "definitely_not_a_real_profile_xyz",
                additionalSearchPathList: Path.GetTempPath());
            Assert.Null(result);
        }

        [Fact]
        public void FilesystemProfileProvider_NullName_ThrowsArgumentNullException()
        {
            var provider = new FilesystemProfileProvider();
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
                var provider = new FilesystemProfileProvider();
                IConnectionProfile? profile = provider.GetProfile("myprofile", additionalSearchPathList: dir);

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
            var provider = new FilesystemProfileProvider();
            IConnectionProfile? profile = provider.GetProfile(path);

            Assert.NotNull(profile);
            Assert.Equal("abs_driver", profile!.DriverName);
        }

        // -----------------------------------------------------------------------
        // Positive: driver_type field in TOML profile
        // -----------------------------------------------------------------------

        [Fact]
        public void ParseProfile_WithDriverType_ParsedCorrectly()
        {
            const string toml = @"
version = 1
driver = ""Apache.Arrow.Adbc.Tests.dll""
driver_type = ""Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDriver""
";
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            Assert.Equal("Apache.Arrow.Adbc.Tests.dll", profile.DriverName);
            Assert.Equal("Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDriver", profile.DriverTypeName);
        }

        [Fact]
        public void ParseProfile_WithoutDriverType_DriverTypeNameIsNull()
        {
            const string toml = @"
version = 1
driver = ""mydriver""
";
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            Assert.Null(profile.DriverTypeName);
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
            Assert.IsType<FakeAdbcDriver>(driver);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            IReadOnlyDictionary<string, string> opts = AdbcDriverManager.BuildStringOptions(profile);

            Assert.Empty(opts);
        }

        // -----------------------------------------------------------------------
        // Positive: OpenDatabaseFromProfile end-to-end with managed driver
        // -----------------------------------------------------------------------

        [Fact]
        public void OpenDatabaseFromProfile_ManagedDriver_OpensDatabase()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            // Build TOML content; escape any backslashes in the Windows assembly path.
            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"
                + "\n[options]\n"
                + "project_id = \"my-project\"\n"
                + "region = \"us-east1\"\n";

            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile);

            FakeAdbcDatabase fakeDb = Assert.IsType<FakeAdbcDatabase>(db);
            Assert.Equal("my-project", fakeDb.Parameters["project_id"]);
            Assert.Equal("us-east1", fakeDb.Parameters["region"]);
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
        public void OpenDatabaseFromProfile_ManagedDriverMissingAssemblyPath_ThrowsAdbcException()
        {
            // driver_type is set but the driver (assembly path) field is omitted.
            const string toml = @"
version = 1
driver_type = ""Apache.Arrow.Adbc.Tests.DriverManager.FakeAdbcDriver""

[options]
key = ""value""
";
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
        public void LoadManagedDriver_NonExistentAssemblyFile_ThrowsAdbcExceptionWithIOErrorStatus()
        {
            string missingPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".dll");
            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(missingPath, "Some.Type"));
            Assert.Equal(AdbcStatusCode.IOError, ex.Status);
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
                () => TomlConnectionProfile.FromContent(toml));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void ParseProfile_VersionIsQuotedNonNumericString_ThrowsAdbcException()
        {
            // A quoted string that cannot be converted to long must not leak FormatException.
            const string toml = "version = \"abc\"\ndriver = \"d\"\n";
            AdbcException ex = Assert.Throws<AdbcException>(
                () => TomlConnectionProfile.FromContent(toml));
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            Assert.Equal("second", profile.StringOptions["key"]);
        }

        // -----------------------------------------------------------------------
        // ResolveEnvVars preserves DriverTypeName
        // -----------------------------------------------------------------------

        [Fact]
        public void ResolveEnvVars_DriverTypeNameIsPreserved()
        {
            const string varName = "ADBC_TEST_RESOLVE_ENVVAR_HOST";
            Environment.SetEnvironmentVariable(varName, "myhost");
            try
            {
                const string toml = @"
version = 1
driver = ""MyDriver.dll""
driver_type = ""My.Namespace.MyDriver""

[options]
host = ""env_var(ADBC_TEST_RESOLVE_ENVVAR_HOST)""
";
                TomlConnectionProfile resolved = TomlConnectionProfile.FromContent(toml).ResolveEnvVars();
                Assert.Equal("My.Namespace.MyDriver", resolved.DriverTypeName);
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
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"
                + "\n[options]\n"
                + "known_key = \"hello\"\n"
                + "unknown_widget = \"ignored_by_driver\"\n";

            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile);

            FakeAdbcDatabase fakeDb = Assert.IsType<FakeAdbcDatabase>(db);
            // Both keys arrive at the driver; the driver decides what to do with them.
            Assert.Equal("hello", fakeDb.Parameters["known_key"]);
            Assert.Equal("ignored_by_driver", fakeDb.Parameters["unknown_widget"]);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);

            var explicitOptions = new Dictionary<string, string>
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            var empty = new Dictionary<string, string>();
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
            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);

            var explicitOptions = new Dictionary<string, string>
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
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"
                + "\n[options]\n"
                + "profile_option = \"from_profile\"\n"
                + "shared_option = \"profile_value\"\n";

            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);

            var explicitOptions = new Dictionary<string, string>
            {
                { "explicit_option", "from_explicit" },
                { "shared_option", "explicit_value" }
            };

            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile, explicitOptions);

            FakeAdbcDatabase fakeDb = Assert.IsType<FakeAdbcDatabase>(db);

            // Profile-only option should be present
            Assert.Equal("from_profile", fakeDb.Parameters["profile_option"]);

            // Explicit-only option should be present
            Assert.Equal("from_explicit", fakeDb.Parameters["explicit_option"]);

            // Shared option: explicit should override profile
            Assert.Equal("explicit_value", fakeDb.Parameters["shared_option"]);
        }

        [Fact]
        public void OpenDatabaseFromProfile_NullExplicitOptions_UsesOnlyProfile()
        {
            string assemblyPath = typeof(FakeAdbcDriver).Assembly.Location;
            string typeName = typeof(FakeAdbcDriver).FullName!;

            string escapedPath = assemblyPath.Replace("\\", "\\\\");
            string toml = "version = 1\n"
                + "driver = \"" + escapedPath + "\"\n"
                + "driver_type = \"" + typeName + "\"\n"
                + "\n[options]\n"
                + "key = \"value\"\n";

            TomlConnectionProfile profile = TomlConnectionProfile.FromContent(toml);
            AdbcDatabase db = AdbcDriverManager.OpenDatabaseFromProfile(profile, null);

            FakeAdbcDatabase fakeDb = Assert.IsType<FakeAdbcDatabase>(db);
            Assert.Equal("value", fakeDb.Parameters["key"]);
        }
    }
}
