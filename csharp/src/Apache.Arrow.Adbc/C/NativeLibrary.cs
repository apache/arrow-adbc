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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.C
{
    internal static class NativeLibrary
    {
        static readonly Loader _loader;

        static NativeLibrary()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                _loader = new Windows();
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                _loader = new OSX();
            }
            else
            {
                _loader = new Unsupported();
            }
        }

        private interface Loader
        {
            IntPtr Load(string libraryPath);
            IntPtr GetExport(IntPtr handle, string name);
            void Free(IntPtr handle);
        }

        sealed private class OSX : Loader
        {
            private const string libdl = "libdl.dylib";
            private const int RTLD_NOW = 2;

            [DllImport(libdl)]
            static extern IntPtr dlopen(string fileName, int flags);

            [DllImport(libdl)]
            static extern IntPtr dlsym(IntPtr libraryHandle, string symbol);

            [DllImport(libdl)]
            static extern int dlclose(IntPtr handle);

            public IntPtr Load(string libraryPath) => dlopen(libraryPath, RTLD_NOW);
            public IntPtr GetExport(IntPtr handle, string name) => dlsym(handle, name);
            public void Free(IntPtr handle) => dlclose(handle);
        }

        sealed private class Windows : Loader
        {
            private const string kernel32 = "kernel32.dll";
            private const int LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x1000;
            private const int LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x0100;

            [DllImport(kernel32)]
            [return: MarshalAs(UnmanagedType.Bool)]
            static extern bool FreeLibrary(IntPtr libraryHandle);

            [DllImport(kernel32, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
            static extern IntPtr GetProcAddress(IntPtr libraryHandle, string functionName);

            [DllImport(kernel32, CharSet = CharSet.Unicode, SetLastError = true)]
            static extern IntPtr LoadLibraryEx(string fileName, IntPtr hFile, uint flags);

            public IntPtr Load(string fileName) =>
                LoadLibraryEx(fileName, IntPtr.Zero, LOAD_LIBRARY_SEARCH_DEFAULT_DIRS | LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR);
            public IntPtr GetExport(IntPtr handle, string name) => GetProcAddress(handle, name);
            public void Free(IntPtr handle) => FreeLibrary(handle);
        }

        sealed private class Unsupported : Loader
        {
            public void Free(IntPtr handle) => throw new NotSupportedException();
            public IntPtr GetExport(IntPtr handle, string name) => throw new NotSupportedException();
            public IntPtr Load(string libraryPath) => throw new NotSupportedException();
        }

        public static IntPtr Load(string fileName) => _loader.Load(fileName);
        public static IntPtr GetExport(IntPtr handle, string name) => _loader.GetExport(handle, name);
        public static void Free(IntPtr handle) => _loader.Free(handle);
    }
}
