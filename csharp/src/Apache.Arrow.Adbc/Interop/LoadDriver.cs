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
using Apache.Arrow.Adbc.Core;
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
        const string driverInit = "AdbcDriverInit";

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
                    return dlclose(this.handle) == 0;
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
                    return FreeLibrary(this.handle);
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
            NativeAdbcDriver nativeDriver;

            public AdbcDriverNative(NativeAdbcDriver nativeDriver)
            {
                this.nativeDriver = nativeDriver;
            }

            /// <summary>
            /// Opens a database
            /// </summary>
            /// <param name="parameters"></param>
            /// <returns></returns>
            public override AdbcDatabase Open(Dictionary<string, string> parameters)
            {
                NativeAdbcDatabase nativeDatabase = new NativeAdbcDatabase();
                using (ErrorHelper error = new ErrorHelper())
                {
                    error.Call(
                        Marshal.GetDelegateForFunctionPointer<DatabaseFn>(this.nativeDriver.DatabaseNew),
                        ref nativeDatabase);

                    DatabaseSetOption setOption = Marshal.GetDelegateForFunctionPointer<DatabaseSetOption>(this.nativeDriver.DatabaseSetOption);
                    if (parameters != null)
                    {
                        foreach (KeyValuePair<string, string> pair in parameters)
                        {
                            error.Call(setOption, ref nativeDatabase, pair.Key, pair.Value);
                        }
                    }
                    error.Call(Marshal.GetDelegateForFunctionPointer<DatabaseFn>(this.nativeDriver.DatabaseInit), ref nativeDatabase);
                }

                return new AdbcDatabaseNative(this.nativeDriver, nativeDatabase);
            }

            public override void Dispose()
            {
                if (this.nativeDriver.release != IntPtr.Zero)
                {
                    using (ErrorHelper error = new ErrorHelper())
                    {
                        try
                        {
                            error.Call(Marshal.GetDelegateForFunctionPointer<DriverRelease>(this.nativeDriver.release), ref this.nativeDriver);
                        }
                        finally
                        {
                            this.nativeDriver.release = IntPtr.Zero;
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
            NativeAdbcDriver nativeDriver;
            NativeAdbcDatabase nativeDatabase;

            public AdbcDatabaseNative(NativeAdbcDriver nativeDriver, NativeAdbcDatabase nativeDatabase)
            {
                this.nativeDriver = nativeDriver;
                this.nativeDatabase = nativeDatabase;
            }

            public override AdbcConnection Connect(Dictionary<string, string> options)
            {
                NativeAdbcConnection nativeConnection = new NativeAdbcConnection();
                using (ErrorHelper error = new ErrorHelper())
                {
                    error.Call(
                        Marshal.GetDelegateForFunctionPointer<ConnectionFn>(this.nativeDriver.ConnectionNew),
                        ref nativeConnection);
                    ConnectionSetOption setOption = Marshal.GetDelegateForFunctionPointer<ConnectionSetOption>(this.nativeDriver.ConnectionSetOption);
                    if (options != null)
                    {
                        foreach (KeyValuePair<string, string> pair in options)
                        {
                            error.Call(setOption, ref nativeConnection, pair.Key, pair.Value);
                        }
                    }
                    error.Call(Marshal.GetDelegateForFunctionPointer<ConnectionInit>(this.nativeDriver.ConnectionInit), ref nativeConnection, ref this.nativeDatabase);
                }

                return new AdbcConnectionNative(this.nativeDriver, nativeConnection);
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
            NativeAdbcDriver nativeDriver;
            NativeAdbcConnection nativeConnection;

            public AdbcConnectionNative(NativeAdbcDriver nativeDriver, NativeAdbcConnection nativeConnection)
            {
                this.nativeDriver = nativeDriver;
                this.nativeConnection = nativeConnection;
            }

            public override AdbcStatement CreateStatement()
            {
                NativeAdbcStatement nativeStatement = new NativeAdbcStatement();
                using (ErrorHelper error = new ErrorHelper())
                {
                    error.Call(
                        Marshal.GetDelegateForFunctionPointer<StatementNew>(this.nativeDriver.StatementNew),
                        ref this.nativeConnection,
                        ref nativeStatement);
                }

                return new AdbcStatementNative(this.nativeDriver, nativeStatement);
            }

        }

        /// <summary>
        /// A native implementation of <see cref="AdbcStatement"/>
        /// </summary>
        sealed class AdbcStatementNative : AdbcStatement
        {
            NativeAdbcDriver nativeDriver;
            NativeAdbcStatement nativeStatement;

            public AdbcStatementNative(NativeAdbcDriver nativeDriver, NativeAdbcStatement nativeStatement)
            {
                this.nativeDriver = nativeDriver;
                this.nativeStatement = nativeStatement;
            }

            public unsafe override QueryResult ExecuteQuery()
            {
                CArrowArrayStream* nativeArrayStream = CArrowArrayStream.Create();
                using (ErrorHelper error = new ErrorHelper())
                {
                    error.Call(
                        Marshal.GetDelegateForFunctionPointer<StatementSetSqlQuery>(this.nativeDriver.StatementSetSqlQuery),
                        ref this.nativeStatement,
                        this.SqlQuery);

                    long rows = 0;
                    error.Call(
                        Marshal.GetDelegateForFunctionPointer<StatementExecuteQuery>(this.nativeDriver.StatementExecuteQuery),
                        ref this.nativeStatement,
                        nativeArrayStream,
                        ref rows);

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
            IntPtr s;

            public Utf8Helper(string s) 
            {
                #if NETSTANDARD
                    this.s = MarshalExtensions.StringToCoTaskMemUTF8(s);
                #else
                    this.s = Marshal.StringToCoTaskMemUTF8(s); 
                #endif
            }

            public static implicit operator IntPtr(Utf8Helper s) { return s.s; }
            public void Dispose() { Marshal.FreeCoTaskMem(this.s); }
        }

        /// <summary>
        /// Assists with error marshalling
        /// </summary>
        struct ErrorHelper : IDisposable
        {
            NativeAdbcError error;

            public unsafe void Call<T1>(Func<T1, IntPtr, AdbcStatusCode> fn, T1 arg1)
            {
                fixed (void* err = &this.error)
                {
                    fn(arg1, (IntPtr)err);
                }
            }

            public void Call(DriverRelease fn, ref NativeAdbcDriver nativeDriver)
            {
                TranslateError(fn(ref nativeDriver, ref this.error));
            }

            public void Call(DatabaseFn fn, ref NativeAdbcDatabase nativeDatabase)
            {
                TranslateError(fn(ref nativeDatabase, ref this.error));
            }

            public void Call(DatabaseSetOption fn, ref NativeAdbcDatabase nativeDatabase, string key, string value)
            {
                using (Utf8Helper utf8Key = new Utf8Helper(key))
                using (Utf8Helper utf8Value = new Utf8Helper(value))
                {
                    TranslateError(fn(ref nativeDatabase, utf8Key, utf8Value, ref this.error));
                }
            }

            public void Call(ConnectionFn fn, ref NativeAdbcConnection nativeConnection)
            {
                TranslateError(fn(ref nativeConnection, ref this.error));
            }

            public void Call(ConnectionSetOption fn, ref NativeAdbcConnection nativeConnection, string key, string value)
            {
                using (Utf8Helper utf8Key = new Utf8Helper(key))
                using (Utf8Helper utf8Value = new Utf8Helper(value))
                {
                    TranslateError(fn(ref nativeConnection, utf8Key, utf8Value, ref this.error));
                }
            }

            public void Call(ConnectionInit fn, ref NativeAdbcConnection nativeConnection, ref NativeAdbcDatabase database)
            {
                TranslateError(fn(ref nativeConnection, ref database, ref this.error));
            }

            public void Call(StatementNew fn, ref NativeAdbcConnection nativeConnection, ref NativeAdbcStatement nativeStatement)
            {
                TranslateError(fn(ref nativeConnection, ref nativeStatement, ref this.error));
            }

            public void Call(StatementFn fn, ref NativeAdbcStatement nativeStatement)
            {
                TranslateError(fn(ref nativeStatement, ref this.error));
            }

            public void Call(StatementSetSqlQuery fn, ref NativeAdbcStatement nativeStatement, string sqlQuery)
            {
                using (Utf8Helper query = new Utf8Helper(sqlQuery))
                {
                    TranslateError(fn(ref nativeStatement, query, ref this.error));
                }
            }

            public unsafe void Call(StatementExecuteQuery fn, ref NativeAdbcStatement nativeStatement, CArrowArrayStream* arrowStream, ref long nRows)
            {
                fixed (long* rows = &nRows)
                {
                    TranslateError(fn(ref nativeStatement, arrowStream, rows, ref this.error));
                }
            }

            public unsafe void Dispose()
            {
                if (error.release != IntPtr.Zero)
                {
                    fixed (NativeAdbcError* err = &this.error)
                    {
                        Marshal.GetDelegateForFunctionPointer<ErrorRelease>(error.release)(err);
                        error.release = IntPtr.Zero;
                    }
                }
            }

            void TranslateError(AdbcStatusCode statusCode)
            {
                if (statusCode != AdbcStatusCode.Success)
                {
                    string message = "Undefined error";
                    if (this.error.message != IntPtr.Zero)
                    {
                        #if NETSTANDARD
                            message = MarshalExtensions.PtrToStringUTF8(this.error.message);
                        #else
                            message = Marshal.PtrToStringUTF8(this.error.message);
                        #endif
                    }
                    Dispose();
                    throw new AdbcException(message);
                }
            }
        }
    }
}
