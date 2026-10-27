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
    /// </summary>
    [Collection(DriverManagerSecurityCollection.Name)]
    public class DriverNotFoundTests
    {
        private const string MissingDriverName = "adbc_test_driver_manager_missing_driver_7c8f5482";

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
            AssertNotFound(Path.Combine(MissingDriverDir(), PlatformLibraryName(MissingDriverName)), flags);
        }

        [Fact]
        public void RelativePathWithLibraryExtension_ThrowsNotFound()
        {
            AssertNotFound(PlatformLibraryName(MissingDriverName), AdbcLoadFlags.Compatible);
        }

        [Theory]
        [InlineData(AdbcLoadFlags.Default)]
        [InlineData(AdbcLoadFlags.None)]
        public void AbsoluteManifestPath_ThrowsNotFound(AdbcLoadFlags flags)
        {
            AssertNotFound(Path.Combine(MissingDriverDir(), MissingDriverName + ".toml"), flags);
        }

        [Fact]
        public void RelativeManifestPath_ThrowsNotFound()
        {
            AssertNotFound(MissingDriverName + ".toml", AdbcLoadFlags.Compatible);
        }

        private static void AssertNotFound(string driverName, AdbcLoadFlags flags, string? additionalSearchPathList = null)
        {
            Exception? ex = Record.Exception(
                () => AdbcDriverManager.FindLoadDriver(
                    driverName,
                    loadOptions: flags,
                    additionalSearchPathList: additionalSearchPathList));

            Assert.True(
                ex is AdbcException,
                $"Expected AdbcException(NotFound), got {ex?.GetType().FullName ?? "no exception"}: {ex?.Message}");
            Assert.Equal(AdbcStatusCode.NotFound, ((AdbcException)ex!).Status);
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
