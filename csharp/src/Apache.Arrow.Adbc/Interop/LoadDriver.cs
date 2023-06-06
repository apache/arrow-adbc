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
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Apache.Arrow.Adbc;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Win32.SafeHandles;

#if NETSTANDARD
using Apache.Arrow.Adbc.Extensions;
#endif

namespace Apache.Arrow.Adbc.Interop
{
    public delegate byte AdbcDriverInit(int version, ref NativeAdbcDriver driver, ref NativeAdbcError error);

    /// <summary>
    /// Class for working with loading drivers from files
    /// </summary>
    public static class LoadDriver
    {
        private const string driverInit = "AdbcDriverInit";

        class NativeDriver
        {
            public SafeHandle driverHandle;
            public NativeAdbcDriver driver;
        }

        /// <summary>
        /// Class used for Mac interoperability
        /// </summary>
        static class MacInterop
        {
            const string libdl = "libdl.dylib";

            [DllImport(libdl)]
            static extern SafeLibraryHandle dlopen(string fileName, int flags);

            [DllImport(libdl)]
            static extern IntPtr dlsym(SafeHandle libraryHandle, string symbol);

            [DllImport(libdl)]
            static extern int dlclose(IntPtr handle);

            sealed class SafeLibraryHandle : SafeHandleZeroOrMinusOneIsInvalid
            {
                SafeLibraryHandle() : base(true) { }

                protected override bool ReleaseHandle()
                {
                    return dlclose(handle) == 0;
                }
            }

            public static NativeDriver GetDriver(string file)
            {
                SafeHandle library = dlopen(file, 2); // TODO: find a symbol for 2
                IntPtr symbol = dlsym(library, "AdbcDriverInit");
                AdbcDriverInit init = Marshal.GetDelegateForFunctionPointer<AdbcDriverInit>(symbol);
                NativeAdbcDriver driver = new NativeAdbcDriver();
                NativeAdbcError error = new NativeAdbcError();
                byte result = init(1000000, ref driver, ref error);
                return new NativeDriver { driverHandle = library, driver = driver };
            }
        }

        /// <summary>
        /// Class used for Windows interoperability
        /// </summary>
        static class WindowsInterop
        {
            const string kernel32 = "kernel32.dll";

            [DllImport(kernel32)]
            [return: MarshalAs(UnmanagedType.Bool)]
            static extern bool FreeLibrary(IntPtr libraryHandle);

            [DllImport(kernel32, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
            static extern IntPtr GetProcAddress(SafeHandle libraryHandle, string functionName);

            [DllImport(kernel32, CharSet = CharSet.Unicode, SetLastError = true)]
            static extern SafeLibraryHandle LoadLibraryEx(string fileName, IntPtr hFile, uint flags);

            sealed class SafeLibraryHandle : SafeHandleZeroOrMinusOneIsInvalid
            {
                SafeLibraryHandle() : base(true) { }

                protected override bool ReleaseHandle()
                {
                    return FreeLibrary(handle);
                }
            }

            public static NativeDriver GetDriver(string file)
            {
                SafeHandle library = LoadLibraryEx(file, IntPtr.Zero, 0x1100);
                IntPtr symbol = GetProcAddress(library, "AdbcDriverInit");
                AdbcDriverInit init = Marshal.GetDelegateForFunctionPointer<AdbcDriverInit>(symbol);
                NativeAdbcDriver driver = new NativeAdbcDriver();
                NativeAdbcError error = new NativeAdbcError();
                byte result = init(1000000, ref driver, ref error);
                return new NativeDriver { /* driverHandle = library, */ driver = driver };
            }
        }

        /// <summary>
        /// Loads an <see cref="AdbcDriver"/> from the file system.
        /// </summary>
        /// <param name="file">The path to the file</param>
        /// <returns></returns>
        public static AdbcDriver Load(string file)
        {
            if (file[0] == '/')
            {
                return new AdbcDriverNative(MacInterop.GetDriver(file).driver);
            }
            else
            {
                return new AdbcDriverNative(WindowsInterop.GetDriver(file).driver);
            }
        }

        /// <summary>
        /// Native implementation of <see cref="AdbcDriver"/>
        /// </summary>
        sealed class AdbcDriverNative : AdbcDriver
        {
            private NativeAdbcDriver _nativeDriver;

            public AdbcDriverNative(NativeAdbcDriver nativeDriver)
            {
                _nativeDriver = nativeDriver;
            }

            /// <summary>
            /// Opens a database
            /// </summary>
            /// <param name="parameters"></param>
            /// <returns></returns>
            public unsafe override AdbcDatabase Open(Dictionary<string, string> parameters)
            {
                NativeAdbcDatabase nativeDatabase = new NativeAdbcDatabase();
        
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.DatabaseNew, ref nativeDatabase);

                    if (parameters != null)
                    {
                        foreach (KeyValuePair<string, string> pair in parameters)
                        {
                            caller.Call(_nativeDriver.DatabaseSetOption, ref nativeDatabase, pair.Key, pair.Value);
                        }
                    }

                    caller.Call(_nativeDriver.DatabaseInit, ref nativeDatabase);
                }

                return new AdbcDatabaseNative(_nativeDriver, nativeDatabase);
            }

            public unsafe override void Dispose()
            {
                if (_nativeDriver.release != null)
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        try
                        {
                            caller.Call(_nativeDriver.release, ref _nativeDriver);
                        }
                        finally
                        {
                            _nativeDriver.release = null;
                        }
                    }
                    base.Dispose();
                }
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcDatabase"/>
        /// </summary>
        internal sealed class AdbcDatabaseNative : AdbcDatabase
        {
            private NativeAdbcDriver _nativeDriver;
            private NativeAdbcDatabase _nativeDatabase;

            public AdbcDatabaseNative(NativeAdbcDriver nativeDriver, NativeAdbcDatabase nativeDatabase)
            {
                _nativeDriver = nativeDriver;
                _nativeDatabase = nativeDatabase;
            }

            public unsafe override AdbcConnection Connect(Dictionary<string, string> options)
            {
                NativeAdbcConnection nativeConnection = new NativeAdbcConnection();
               
                using(CallHelper caller = new CallHelper())
                {
                    if (options != null)
                    {
                        foreach (KeyValuePair<string, string> pair in options)
                        {
                            caller.Call(_nativeDriver.ConnectionSetOption, ref nativeConnection, pair.Key, pair.Value);
                        }
                    }

                    caller.Call(_nativeDriver.ConnectionInit, ref nativeConnection, ref _nativeDatabase);
                }

                return new AdbcConnectionNative(_nativeDriver, nativeConnection);
            }

            public override void Dispose()
            {
                base.Dispose();
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcConnection"/>
        /// </summary>
        internal sealed class AdbcConnectionNative : AdbcConnection
        {
            private NativeAdbcDriver _nativeDriver;
            private NativeAdbcConnection _nativeConnection;

            public AdbcConnectionNative(NativeAdbcDriver nativeDriver, NativeAdbcConnection nativeConnection)
            {
                _nativeDriver = nativeDriver;
                _nativeConnection = nativeConnection;
            }

            public unsafe override AdbcStatement CreateStatement()
            {
                NativeAdbcStatement nativeStatement = new NativeAdbcStatement();

                using(CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.StatementNew, ref _nativeConnection, ref nativeStatement);
                }

                return new AdbcStatementNative(_nativeDriver, nativeStatement);
            }

        }

        /// <summary>
        /// A native implementation of <see cref="AdbcStatement"/>
        /// </summary>
        sealed class AdbcStatementNative : AdbcStatement
        {
            private NativeAdbcDriver _nativeDriver;
            private NativeAdbcStatement _nativeStatement;

            public AdbcStatementNative(NativeAdbcDriver nativeDriver, NativeAdbcStatement nativeStatement)
            {
                _nativeDriver = nativeDriver;
                _nativeStatement = nativeStatement;
            }

            public unsafe override QueryResult ExecuteQuery()
            {
                CArrowArrayStream* nativeArrayStream = CArrowArrayStream.Create();
                
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.StatementSetSqlQuery, ref _nativeStatement, SqlQuery);
                  
                    long rows = 0;

                    caller.Call(_nativeDriver.StatementExecuteQuery, ref _nativeStatement, nativeArrayStream, ref rows);

                    return new QueryResult(rows, CArrowArrayStreamImporter.ImportArrayStream(nativeArrayStream));
                }
            }

            public override unsafe UpdateResult ExecuteUpdate()
            {
                throw AdbcException.NotImplemented("Driver does not support ExecuteUpdate");
            }

            public override object GetValue(IArrowArray arrowArray, Field field, int index)
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Assists with UTF8/string marshalling
        /// </summary>
        struct Utf8Helper : IDisposable
        {
            private IntPtr _s;

            public Utf8Helper(string s) 
            {
                #if NETSTANDARD
                    _s = MarshalExtensions.StringToCoTaskMemUTF8(s);
                #else
                    _s = Marshal.StringToCoTaskMemUTF8(s); 
                #endif
            }

            public static implicit operator IntPtr(Utf8Helper s) { return s._s; }
            public void Dispose() { Marshal.FreeCoTaskMem(_s); }
        }

        /// <summary>
        /// Assists with delegate calls and handling error codes
        /// </summary>
        struct CallHelper : IDisposable
        {
            private NativeAdbcError _error;

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcDriver*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcDriver nativeDriver)
            {
                fixed (NativeAdbcDriver* driver = &nativeDriver)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(driver, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcDatabase nativeDatabase)
            {
                fixed (NativeAdbcDatabase* db = &nativeDatabase)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(db, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcDatabase nativeDatabase, string key, string value)
            {
                fixed (NativeAdbcDatabase* db = &nativeDatabase)
                fixed (NativeAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        unsafe
                        {
                            IntPtr keyPtr = utf8Key;
                            IntPtr valuePtr = utf8Value;

                            TranslateCode(fn(db, (byte*)keyPtr, (byte*)valuePtr, e));
                        }
                    }
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcConnection nativeConnection)
            {
                fixed (NativeAdbcConnection* cn = &nativeConnection)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcConnection nativeConnection, string key, string value)
            {
                fixed (NativeAdbcConnection* cn = &nativeConnection)
                fixed (NativeAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        unsafe
                        {
                            IntPtr keyPtr = utf8Key;
                            IntPtr valuePtr = utf8Value;

                            TranslateCode(fn(cn, (byte*)keyPtr, (byte*)valuePtr, e));
                        }
                    }
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcConnection nativeConnection, ref NativeAdbcDatabase database)
            {
                fixed (NativeAdbcConnection* cn = &nativeConnection)
                fixed (NativeAdbcDatabase* db = &database)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, db, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcConnection nativeConnection, ref NativeAdbcStatement nativeStatement)
            {
                fixed (NativeAdbcConnection* cn = &nativeConnection)
                fixed (NativeAdbcStatement* stmt = &nativeStatement)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, stmt, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcStatement nativeStatement)
            {
                fixed (NativeAdbcStatement* stmt = &nativeStatement)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcStatement nativeStatement, string sqlQuery)
            {
                fixed (NativeAdbcStatement* stmt = &nativeStatement)
                fixed (NativeAdbcError* e = &_error)
                {
                    using (Utf8Helper query = new Utf8Helper(sqlQuery))
                    {
                        IntPtr bQuery = (IntPtr)(query);

                        TranslateCode(fn(stmt, (byte*)bQuery, e));
                    }
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, long*, NativeAdbcError*, AdbcStatusCode> fn, ref NativeAdbcStatement nativeStatement, CArrowArrayStream* arrowStream, ref long nRows)
            {
                fixed (NativeAdbcStatement* stmt = &nativeStatement)
                fixed (long* rows = &nRows)
                fixed (NativeAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, arrowStream, rows, e));
                }
            }

            public unsafe void Dispose()
            {
                if (_error.release != null)
                {
                    fixed (NativeAdbcError* err = &_error)
                    {
                        _error.release(err);
                        _error.release = null;
                    }
                }
            }

            internal unsafe void TranslateCode(AdbcStatusCode statusCode)
            {
                if (statusCode != AdbcStatusCode.Success)
                {
                    string message = "Undefined error";
                    if ((IntPtr)_error.message != IntPtr.Zero)
                    {
                        #if NETSTANDARD
                            message = MarshalExtensions.PtrToStringUTF8((IntPtr)_error.message);
                        #else
                            message = Marshal.PtrToStringUTF8((IntPtr)_error.message);
                        #endif
                    }
                    Dispose();
                    throw new AdbcException(message);
                }
            }
        }
    }
}
