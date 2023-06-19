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
using Apache.Arrow.C;

#if NETSTANDARD
using Apache.Arrow.Adbc.Extensions;
#endif

namespace Apache.Arrow.Adbc.C
{
    internal delegate AdbcStatusCode AdbcDriverInit(int version, ref CAdbcDriver driver, ref CAdbcError error);

    /// <summary>
    /// Class for working with imported drivers from files
    /// </summary>
    public static class CAdbcDriverImporter
    {
        private const string driverInit = "AdbcDriverInit";
        private const int ADBC_VERSION_1_0_0 = 1000000;

        /// <summary>
        /// Loads an <see cref="AdbcDriver"/> from the file system.
        /// </summary>
        /// <param name="file">
        /// The path to the file.
        /// </param>
        public static AdbcDriver Load(string file)
        {
            if (file == null)
            {
                throw new ArgumentNullException(nameof(file));
            }

            if (!File.Exists(file))
            {
                throw new ArgumentException("file does not exist", nameof(file));
            }

            IntPtr library = NativeLibrary.Load(file);
            if (library == IntPtr.Zero)
            {
                throw new ArgumentException("unable to load library", nameof(file));
            }

            try
            {
                IntPtr export = NativeLibrary.GetExport(library, driverInit);
                if (export == IntPtr.Zero)
                {
                    NativeLibrary.Free(library);
                    throw new ArgumentException("Unable to find AdbcDriverInit export", nameof(file));
                }

                AdbcDriverInit init = Marshal.GetDelegateForFunctionPointer<AdbcDriverInit>(export);
                CAdbcDriver driver = new CAdbcDriver();
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(init, ADBC_VERSION_1_0_0, ref driver);
                    ImportedAdbcDriver result = new ImportedAdbcDriver(library, driver);
                    library = IntPtr.Zero;
                    return result;
                }
            }
            finally
            {
                if (library != IntPtr.Zero) { NativeLibrary.Free(library); }
            }
        }

        /// <summary>
        /// Native implementation of <see cref="AdbcDriver"/>
        /// </summary>
        sealed class ImportedAdbcDriver : AdbcDriver
        {
            private IntPtr _library;
            private CAdbcDriver _nativeDriver;

            public ImportedAdbcDriver(IntPtr library, CAdbcDriver nativeDriver)
            {
                _library = library;
                _nativeDriver = nativeDriver;
            }

            /// <summary>
            /// Opens a database
            /// </summary>
            /// <param name="parameters">
            /// Parameters to use when calling DatabaseNew.
            /// </param>
            public unsafe override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            {
                CAdbcDatabase nativeDatabase = new CAdbcDatabase();

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

                    NativeLibrary.Free(_library);
                    _library = IntPtr.Zero;

                    base.Dispose();
                }
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcDatabase"/>
        /// </summary>
        internal sealed class AdbcDatabaseNative : AdbcDatabase
        {
            private CAdbcDriver _nativeDriver;
            private CAdbcDatabase _nativeDatabase;

            public AdbcDatabaseNative(CAdbcDriver nativeDriver, CAdbcDatabase nativeDatabase)
            {
                _nativeDriver = nativeDriver;
                _nativeDatabase = nativeDatabase;
            }

            public unsafe override AdbcConnection Connect(IReadOnlyDictionary<string, string> options)
            {
                CAdbcConnection nativeConnection = new CAdbcConnection();

                using (CallHelper caller = new CallHelper())
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
            private CAdbcDriver _nativeDriver;
            private CAdbcConnection _nativeConnection;

            public AdbcConnectionNative(CAdbcDriver nativeDriver, CAdbcConnection nativeConnection)
            {
                _nativeDriver = nativeDriver;
                _nativeConnection = nativeConnection;
            }

            public unsafe override AdbcStatement CreateStatement()
            {
                CAdbcStatement nativeStatement = new CAdbcStatement();

                using (CallHelper caller = new CallHelper())
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
            private CAdbcDriver _nativeDriver;
            private CAdbcStatement _nativeStatement;

            public AdbcStatementNative(CAdbcDriver nativeDriver, CAdbcStatement nativeStatement)
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
        private struct Utf8Helper : IDisposable
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
        private struct CallHelper : IDisposable
        {
            private CAdbcError _error;

            public unsafe void Call(AdbcDriverInit init, int version, ref CAdbcDriver driver)
            {
                TranslateCode(init(version, ref driver, ref this._error));
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcDriver*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDriver nativeDriver)
            {
                fixed (CAdbcDriver* driver = &nativeDriver)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(driver, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(db, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase, string key, string value)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
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

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, string key, string value)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
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

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, ref CAdbcDatabase database)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcDatabase* db = &database)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, db, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, stmt, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcStatement*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, e));
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement, string sqlQuery)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper query = new Utf8Helper(sqlQuery))
                    {
                        IntPtr bQuery = (IntPtr)(query);

                        TranslateCode(fn(stmt, (byte*)bQuery, e));
                    }
                }
            }

            public unsafe void Call(delegate* unmanaged[Stdcall]<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement, CArrowArrayStream* arrowStream, ref long nRows)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (long* rows = &nRows)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, arrowStream, rows, e));
                }
            }

            public unsafe void Dispose()
            {
                if (_error.release != null)
                {
                    fixed (CAdbcError* err = &_error)
                    {
                        _error.release(err);
                        _error.release = null;
                    }
                }
            }

            private unsafe void TranslateCode(AdbcStatusCode statusCode)
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
