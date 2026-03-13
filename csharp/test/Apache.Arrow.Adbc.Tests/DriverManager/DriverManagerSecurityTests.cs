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
    /// Tests for <see cref="DriverManagerSecurity"/> functionality.
    /// </summary>
    public class DriverManagerSecurityTests : IDisposable
    {
        private readonly List<string> _tempDirs = new List<string>();

        public void Dispose()
        {
            // Reset global state after each test
            DriverManagerSecurity.AuditLogger = null;
            DriverManagerSecurity.Allowlist = null;

            foreach (string dir in _tempDirs)
            {
                try
                {
                    if (Directory.Exists(dir))
                    {
                        Directory.Delete(dir, true);
                    }
                }
                catch { }
            }
        }

        private string CreateTempDirectory()
        {
            string dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            _tempDirs.Add(dir);
            return dir;
        }

        // -----------------------------------------------------------------------
        // Path Traversal Protection Tests
        // -----------------------------------------------------------------------

        [Theory]
        [InlineData("..\\malicious.dll")]
        [InlineData("../malicious.dll")]
        [InlineData("subdir\\..\\..\\malicious.dll")]
        [InlineData("subdir/../../../malicious.dll")]
        [InlineData("foo\\..\\bar\\..\\..\\evil.dll")]
        public void ValidatePathSecurity_PathTraversal_ThrowsException(string maliciousPath)
        {
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManagerSecurity.ValidatePathSecurity(maliciousPath, "testPath"));

            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Contains("..", ex.Message);
        }

        [Theory]
        [InlineData("driver.dll")]
        [InlineData("subdir\\driver.dll")]
        [InlineData("subdir/driver.dll")]
        [InlineData("C:\\Program Files\\Driver\\driver.dll")]
        [InlineData("/usr/lib/driver.so")]
        public void ValidatePathSecurity_ValidPath_DoesNotThrow(string validPath)
        {
            // Should not throw
            DriverManagerSecurity.ValidatePathSecurity(validPath, "testPath");
        }

        [Fact]
        public void ValidatePathSecurity_NullByte_ThrowsException()
        {
            string pathWithNull = "driver\0.dll";

            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManagerSecurity.ValidatePathSecurity(pathWithNull, "testPath"));

            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Contains("null", ex.Message.ToLower());
        }

        [Fact]
        public void ValidatePathSecurity_NullOrEmpty_DoesNotThrow()
        {
            // Null and empty should be allowed (validation happens elsewhere)
            DriverManagerSecurity.ValidatePathSecurity(null!, "testPath");
            DriverManagerSecurity.ValidatePathSecurity(string.Empty, "testPath");
        }

        // -----------------------------------------------------------------------
        // Manifest Path Resolution Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void ValidateAndResolveManifestPath_ValidRelativePath_ResolvesCorrectly()
        {
            string manifestDir = CreateTempDirectory();
            string subDir = Path.Combine(manifestDir, "drivers");
            Directory.CreateDirectory(subDir);

            string resolved = DriverManagerSecurity.ValidateAndResolveManifestPath(
                manifestDir, "drivers\\mydriver.dll");

            Assert.Equal(Path.Combine(manifestDir, "drivers", "mydriver.dll"), resolved);
        }

        [Fact]
        public void ValidateAndResolveManifestPath_PathTraversal_ThrowsException()
        {
            string manifestDir = CreateTempDirectory();

            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManagerSecurity.ValidateAndResolveManifestPath(
                    manifestDir, "..\\..\\malicious.dll"));

            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void ValidateAndResolveManifestPath_EscapesDirectory_ThrowsException()
        {
            string manifestDir = CreateTempDirectory();

            // Even without .. in the literal string, symlinks or other means
            // could cause path escape - the method should catch this
            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManagerSecurity.ValidateAndResolveManifestPath(
                    manifestDir, "subdir\\..\\..\\escape.dll"));

            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        // -----------------------------------------------------------------------
        // Directory Allowlist Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void DirectoryAllowlist_DriverInAllowedDirectory_ReturnsTrue()
        {
            string allowedDir = CreateTempDirectory();
            DirectoryAllowlist allowlist = new DirectoryAllowlist(new[] { allowedDir });

            string driverPath = Path.Combine(allowedDir, "mydriver.dll");

            Assert.True(allowlist.IsDriverAllowed(driverPath, null));
            Assert.True(allowlist.IsDriverAllowed(driverPath, "Some.Type.Name"));
        }

        [Fact]
        public void DirectoryAllowlist_DriverInSubdirectory_ReturnsTrue()
        {
            string allowedDir = CreateTempDirectory();
            string subDir = Path.Combine(allowedDir, "subdir");
            Directory.CreateDirectory(subDir);

            DirectoryAllowlist allowlist = new DirectoryAllowlist(new[] { allowedDir });

            string driverPath = Path.Combine(subDir, "mydriver.dll");

            Assert.True(allowlist.IsDriverAllowed(driverPath, null));
        }

        [Fact]
        public void DirectoryAllowlist_DriverNotInAllowedDirectory_ReturnsFalse()
        {
            string allowedDir = CreateTempDirectory();
            string otherDir = CreateTempDirectory();

            DirectoryAllowlist allowlist = new DirectoryAllowlist(new[] { allowedDir });

            string driverPath = Path.Combine(otherDir, "mydriver.dll");

            Assert.False(allowlist.IsDriverAllowed(driverPath, null));
        }

        [Fact]
        public void DirectoryAllowlist_MultipleDirectories_WorksCorrectly()
        {
            string dir1 = CreateTempDirectory();
            string dir2 = CreateTempDirectory();
            string dir3 = CreateTempDirectory();

            DirectoryAllowlist allowlist = new DirectoryAllowlist(new[] { dir1, dir2 });

            Assert.True(allowlist.IsDriverAllowed(Path.Combine(dir1, "a.dll"), null));
            Assert.True(allowlist.IsDriverAllowed(Path.Combine(dir2, "b.dll"), null));
            Assert.False(allowlist.IsDriverAllowed(Path.Combine(dir3, "c.dll"), null));
        }

        [Fact]
        public void DirectoryAllowlist_EmptyPath_ReturnsFalse()
        {
            string allowedDir = CreateTempDirectory();
            DirectoryAllowlist allowlist = new DirectoryAllowlist(new[] { allowedDir });

            Assert.False(allowlist.IsDriverAllowed(string.Empty, null));
            Assert.False(allowlist.IsDriverAllowed(null!, null));
        }

        // -----------------------------------------------------------------------
        // Type Allowlist Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void TypeAllowlist_AllowedType_ReturnsTrue()
        {
            TypeAllowlist allowlist = new TypeAllowlist(new[]
            {
                "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver",
                "Apache.Arrow.Adbc.Drivers.Snowflake.SnowflakeDriver"
            });

            Assert.True(allowlist.IsDriverAllowed(
                "any_path.dll",
                "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"));
        }

        [Fact]
        public void TypeAllowlist_NotAllowedType_ReturnsFalse()
        {
            TypeAllowlist allowlist = new TypeAllowlist(new[]
            {
                "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"
            });

            Assert.False(allowlist.IsDriverAllowed(
                "any_path.dll",
                "Evil.Malware.Driver"));
        }

        [Fact]
        public void TypeAllowlist_NativeDriver_PermittedByDefault()
        {
            TypeAllowlist allowlist = new TypeAllowlist(new[]
            {
                "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"
            });

            // Native drivers (typeName = null) are permitted by default when no inner allowlist
            Assert.True(allowlist.IsDriverAllowed("native_driver.dll", null));
        }

        [Fact]
        public void TypeAllowlist_NativeDriverWithInnerAllowlist_DelegatesToInner()
        {
            string allowedDir = CreateTempDirectory();
            string otherDir = CreateTempDirectory();

            DirectoryAllowlist dirAllowlist = new DirectoryAllowlist(new[] { allowedDir });
            TypeAllowlist allowlist = new TypeAllowlist(
                new[] { "Some.Type" },
                nativeDriverAllowlist: dirAllowlist);

            // Native driver in allowed directory
            Assert.True(allowlist.IsDriverAllowed(
                Path.Combine(allowedDir, "native.dll"), null));

            // Native driver in disallowed directory
            Assert.False(allowlist.IsDriverAllowed(
                Path.Combine(otherDir, "native.dll"), null));
        }

        // -----------------------------------------------------------------------
        // Composite Allowlist Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void CompositeAllowlist_AllAllowlistsPermit_ReturnsTrue()
        {
            string allowedDir = CreateTempDirectory();

            DirectoryAllowlist dirAllowlist = new DirectoryAllowlist(new[] { allowedDir });
            TypeAllowlist typeAllowlist = new TypeAllowlist(new[] { "Good.Driver" });

            CompositeAllowlist composite = new CompositeAllowlist(dirAllowlist, typeAllowlist);

            Assert.True(composite.IsDriverAllowed(
                Path.Combine(allowedDir, "driver.dll"),
                "Good.Driver"));
        }

        [Fact]
        public void CompositeAllowlist_OneAllowlistDenies_ReturnsFalse()
        {
            string allowedDir = CreateTempDirectory();
            string otherDir = CreateTempDirectory();

            DirectoryAllowlist dirAllowlist = new DirectoryAllowlist(new[] { allowedDir });
            TypeAllowlist typeAllowlist = new TypeAllowlist(new[] { "Good.Driver" });

            CompositeAllowlist composite = new CompositeAllowlist(dirAllowlist, typeAllowlist);

            // Wrong directory
            Assert.False(composite.IsDriverAllowed(
                Path.Combine(otherDir, "driver.dll"),
                "Good.Driver"));

            // Wrong type
            Assert.False(composite.IsDriverAllowed(
                Path.Combine(allowedDir, "driver.dll"),
                "Bad.Driver"));
        }

        // -----------------------------------------------------------------------
        // Audit Logger Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void AuditLogger_CanBeSetAndRetrieved()
        {
            TestAuditLogger logger = new TestAuditLogger();

            DriverManagerSecurity.AuditLogger = logger;

            Assert.Same(logger, DriverManagerSecurity.AuditLogger);
        }

        [Fact]
        public void AuditLogger_SetToNull_DisablesLogging()
        {
            TestAuditLogger logger = new TestAuditLogger();
            DriverManagerSecurity.AuditLogger = logger;

            DriverManagerSecurity.AuditLogger = null;

            Assert.Null(DriverManagerSecurity.AuditLogger);
        }

        [Fact]
        public void LogDriverLoadAttempt_WithLogger_LogsAttempt()
        {
            TestAuditLogger logger = new TestAuditLogger();
            DriverManagerSecurity.AuditLogger = logger;

            DriverLoadAttempt attempt = new DriverLoadAttempt(
                driverPath: "C:\\test\\driver.dll",
                typeName: "Test.Driver",
                manifestPath: "C:\\test\\driver.toml",
                success: true,
                errorMessage: null,
                loadMethod: "LoadDriver");

            DriverManagerSecurity.LogDriverLoadAttempt(attempt);

            Assert.Single(logger.Attempts);
            Assert.Equal("C:\\test\\driver.dll", logger.Attempts[0].DriverPath);
            Assert.Equal("Test.Driver", logger.Attempts[0].TypeName);
            Assert.True(logger.Attempts[0].Success);
        }

        [Fact]
        public void LogDriverLoadAttempt_WithoutLogger_DoesNotThrow()
        {
            DriverManagerSecurity.AuditLogger = null;

            DriverLoadAttempt attempt = new DriverLoadAttempt(
                driverPath: "C:\\test\\driver.dll",
                typeName: null,
                manifestPath: null,
                success: false,
                errorMessage: "Test error",
                loadMethod: "LoadDriver");

            // Should not throw
            DriverManagerSecurity.LogDriverLoadAttempt(attempt);
        }

        [Fact]
        public void LogDriverLoadAttempt_LoggerThrows_DoesNotPropagate()
        {
            ThrowingAuditLogger logger = new ThrowingAuditLogger();
            DriverManagerSecurity.AuditLogger = logger;

            DriverLoadAttempt attempt = new DriverLoadAttempt(
                driverPath: "test.dll",
                typeName: null,
                manifestPath: null,
                success: true,
                errorMessage: null,
                loadMethod: "Test");

            // Should not throw even though logger throws
            DriverManagerSecurity.LogDriverLoadAttempt(attempt);
        }

        // -----------------------------------------------------------------------
        // Allowlist Validation Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void ValidateAllowlist_NoAllowlistConfigured_DoesNotThrow()
        {
            DriverManagerSecurity.Allowlist = null;

            // Should not throw when no allowlist is configured
            DriverManagerSecurity.ValidateAllowlist("any_path.dll", null);
            DriverManagerSecurity.ValidateAllowlist("any_path.dll", "Any.Type");
        }

        [Fact]
        public void ValidateAllowlist_DriverAllowed_DoesNotThrow()
        {
            string allowedDir = CreateTempDirectory();
            DriverManagerSecurity.Allowlist = new DirectoryAllowlist(new[] { allowedDir });

            string driverPath = Path.Combine(allowedDir, "driver.dll");

            // Should not throw
            DriverManagerSecurity.ValidateAllowlist(driverPath, null);
        }

        [Fact]
        public void ValidateAllowlist_DriverNotAllowed_ThrowsUnauthorized()
        {
            string allowedDir = CreateTempDirectory();
            string otherDir = CreateTempDirectory();
            DriverManagerSecurity.Allowlist = new DirectoryAllowlist(new[] { allowedDir });

            string driverPath = Path.Combine(otherDir, "driver.dll");

            AdbcException ex = Assert.Throws<AdbcException>(
                () => DriverManagerSecurity.ValidateAllowlist(driverPath, null));

            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);
            Assert.Contains("allowlist", ex.Message.ToLower());
        }

        // -----------------------------------------------------------------------
        // DriverLoadAttempt Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void DriverLoadAttempt_Constructor_SetsAllProperties()
        {
            DateTime before = DateTime.UtcNow;

            DriverLoadAttempt attempt = new DriverLoadAttempt(
                driverPath: "C:\\drivers\\test.dll",
                typeName: "Test.Driver.Type",
                manifestPath: "C:\\drivers\\test.toml",
                success: false,
                errorMessage: "Test error message",
                loadMethod: "LoadManagedDriver");

            DateTime after = DateTime.UtcNow;

            Assert.Equal("C:\\drivers\\test.dll", attempt.DriverPath);
            Assert.Equal("Test.Driver.Type", attempt.TypeName);
            Assert.Equal("C:\\drivers\\test.toml", attempt.ManifestPath);
            Assert.False(attempt.Success);
            Assert.Equal("Test error message", attempt.ErrorMessage);
            Assert.Equal("LoadManagedDriver", attempt.LoadMethod);
            Assert.True(attempt.TimestampUtc >= before && attempt.TimestampUtc <= after);
        }

        [Fact]
        public void DriverLoadAttempt_NullDriverPath_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new DriverLoadAttempt(
                driverPath: null!,
                typeName: null,
                manifestPath: null,
                success: true,
                errorMessage: null,
                loadMethod: "Test"));
        }

        [Fact]
        public void DriverLoadAttempt_NullLoadMethod_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new DriverLoadAttempt(
                driverPath: "test.dll",
                typeName: null,
                manifestPath: null,
                success: true,
                errorMessage: null,
                loadMethod: null!));
        }

        // -----------------------------------------------------------------------
        // Thread Safety Tests
        // -----------------------------------------------------------------------

        [Fact]
        public void AuditLogger_ConcurrentAccess_IsThreadSafe()
        {
            TestAuditLogger logger1 = new TestAuditLogger();
            TestAuditLogger logger2 = new TestAuditLogger();

            System.Threading.Tasks.Parallel.For(0, 100, i =>
            {
                if (i % 2 == 0)
                {
                    DriverManagerSecurity.AuditLogger = logger1;
                }
                else
                {
                    DriverManagerSecurity.AuditLogger = logger2;
                }

                IDriverLoadAuditLogger? current = DriverManagerSecurity.AuditLogger;
                // Just verify we can read without exception
                Assert.True(current == null || current == logger1 || current == logger2);
            });
        }

        [Fact]
        public void Allowlist_ConcurrentAccess_IsThreadSafe()
        {
            string dir1 = CreateTempDirectory();
            string dir2 = CreateTempDirectory();

            DirectoryAllowlist allowlist1 = new DirectoryAllowlist(new[] { dir1 });
            DirectoryAllowlist allowlist2 = new DirectoryAllowlist(new[] { dir2 });

            System.Threading.Tasks.Parallel.For(0, 100, i =>
            {
                if (i % 2 == 0)
                {
                    DriverManagerSecurity.Allowlist = allowlist1;
                }
                else
                {
                    DriverManagerSecurity.Allowlist = allowlist2;
                }

                IDriverAllowlist? current = DriverManagerSecurity.Allowlist;
                Assert.True(current == null || current == allowlist1 || current == allowlist2);
            });
        }

        // -----------------------------------------------------------------------
        // Helper Classes
        // -----------------------------------------------------------------------

        private class TestAuditLogger : IDriverLoadAuditLogger
        {
            public List<DriverLoadAttempt> Attempts { get; } = new List<DriverLoadAttempt>();

            public void LogDriverLoadAttempt(DriverLoadAttempt attempt)
            {
                Attempts.Add(attempt);
            }
        }

        private class ThrowingAuditLogger : IDriverLoadAuditLogger
        {
            public void LogDriverLoadAttempt(DriverLoadAttempt attempt)
            {
                throw new InvalidOperationException("Intentional test exception");
            }
        }
    }
}
