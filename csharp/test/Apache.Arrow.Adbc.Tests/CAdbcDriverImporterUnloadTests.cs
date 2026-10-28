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
using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class CAdbcDriverImporterUnloadTests : IDisposable
    {
        private readonly string _tempDir =
            Path.Combine(Path.GetTempPath(), "adbc_importer_unload_" + Guid.NewGuid().ToString("N"));

        public CAdbcDriverImporterUnloadTests()
        {
            Directory.CreateDirectory(_tempDir);
        }

        public void Dispose()
        {
            try { Directory.Delete(_tempDir, true); } catch { }
        }

        // Loading with canUnload when the library doesn't export the entrypoint
        // should release the importer's own reference exactly once, leaving any
        // other reference to the library in place
        [Fact]
        public void Load_CanUnload_MissingEntrypoint_KeepsOtherReferences()
        {
            // A copy of the DuckDB library under a unique name, so nothing else in
            // the process has it loaded and this test holds the only other reference
            string duckDb = Path.Combine(Directory.GetCurrentDirectory(), OsLibrary.DuckDbFileName);
            string library = Path.Combine(_tempDir, "adbc_test_unload_" + Guid.NewGuid().ToString("N") + Path.GetExtension(duckDb));
            File.Copy(duckDb, library);

            IntPtr handle = OsLibrary.Load(library);
            Assert.NotEqual(IntPtr.Zero, handle);
            try
            {
                Assert.ThrowsAny<Exception>(
                    () => CAdbcDriverImporter.Load(library, canUnload: true, "AdbcTestNoSuchEntrypoint"));

                Assert.True(
                    OsLibrary.IsLoaded(library),
                    "The library was unloaded while another reference to it was still held");
            }
            finally
            {
                OsLibrary.Free(handle);
            }
        }

        /// <summary>
        /// Loads and checks native libraries directly with the OS loader, which is
        /// also available on .NET Framework.
        /// </summary>
        private static class OsLibrary
        {
            public static string DuckDbFileName =>
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "duckdb.dll"
                : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "libduckdb.dylib"
                : "libduckdb.so";

            public static IntPtr Load(string path) =>
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? Windows.LoadLibraryW(path)
                : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? Mac.dlopen(path, Mac.RTLD_NOW)
                : Linux.dlopen(path, Linux.RTLD_NOW);

            public static void Free(IntPtr handle)
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) Windows.FreeLibrary(handle);
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) Mac.dlclose(handle);
                else Linux.dlclose(handle);
            }

            public static bool IsLoaded(string path)
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    return Windows.GetModuleHandleW(path) != IntPtr.Zero;
                }

                // RTLD_NOLOAD returns a new reference only if the library is already loaded
                IntPtr handle = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                    ? Mac.dlopen(path, Mac.RTLD_NOW | Mac.RTLD_NOLOAD)
                    : Linux.dlopen(path, Linux.RTLD_NOW | Linux.RTLD_NOLOAD);
                if (handle == IntPtr.Zero)
                {
                    return false;
                }
                Free(handle);
                return true;
            }

            private static class Windows
            {
                [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
                public static extern IntPtr LoadLibraryW(string fileName);

                [DllImport("kernel32.dll", SetLastError = true)]
                [return: MarshalAs(UnmanagedType.Bool)]
                public static extern bool FreeLibrary(IntPtr module);

                [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
                public static extern IntPtr GetModuleHandleW(string moduleName);
            }

            private static class Mac
            {
                public const int RTLD_NOW = 0x2;
                public const int RTLD_NOLOAD = 0x10;

                [DllImport("libdl.dylib")]
                public static extern IntPtr dlopen(string fileName, int flags);

                [DllImport("libdl.dylib")]
                public static extern int dlclose(IntPtr handle);
            }

            private static class Linux
            {
                public const int RTLD_NOW = 0x2;
                public const int RTLD_NOLOAD = 0x4;

                [DllImport("libdl.so.2")]
                public static extern IntPtr dlopen(string fileName, int flags);

                [DllImport("libdl.so.2")]
                public static extern int dlclose(IntPtr handle);
            }
        }
    }
}
