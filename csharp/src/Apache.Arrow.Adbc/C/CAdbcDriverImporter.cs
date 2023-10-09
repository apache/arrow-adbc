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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

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
        /// <param name="file">The path to the driver to load</param>
        /// <param name="entryPoint">The name of the entry point. If not provided, the name AdbcDriverInit will be used.</param>
        public static AdbcDriver Load(string file, string entryPoint = null)
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
                entryPoint = entryPoint ?? driverInit;
                IntPtr export = NativeLibrary.GetExport(library, entryPoint);
                if (export == IntPtr.Zero)
                {
                    NativeLibrary.Free(library);
                    throw new ArgumentException($"Unable to find {entryPoint} export", nameof(file));
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
                if (_nativeDriver.release != default)
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        try
                        {
                            caller.Call(_nativeDriver.release, ref _nativeDriver);
                        }
                        finally
                        {
                            _nativeDriver.release = default;
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
                    caller.Call(_nativeDriver.ConnectionNew, ref nativeConnection);

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

            public override IArrowArrayStream GetInfo(List<AdbcInfoCode> codes)
            {
                return GetInfo(codes.Select(x => (int)x).ToList<int>());
            }

            public override unsafe IArrowArrayStream GetInfo(List<int> codes)
            {
                CArrowArrayStream* nativeArrayStream = CArrowArrayStream.Create();

                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionGetInfo, ref _nativeConnection, codes, nativeArrayStream);
                }

                IArrowArrayStream arrowArrayStream = CArrowArrayStreamImporter.ImportArrayStream(nativeArrayStream);

                return arrowArrayStream;
            }

            public override unsafe IArrowArrayStream GetObjects(GetObjectsDepth depth, string catalogPattern, string dbSchemaPattern, string tableNamePattern, List<string> tableTypes, string columnNamePattern)
            {
                CArrowArrayStream* nativeArrayStream = CArrowArrayStream.Create();

                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionGetObjects, ref _nativeConnection, (int)depth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern, nativeArrayStream);
                }

                IArrowArrayStream arrowArrayStream = CArrowArrayStreamImporter.ImportArrayStream(nativeArrayStream);

                return arrowArrayStream;
            }

            public override unsafe IArrowArrayStream GetTableTypes()
            {
                CArrowArrayStream* nativeArrayStream = CArrowArrayStream.Create();

                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionGetTableTypes, ref _nativeConnection, nativeArrayStream);
                }

                IArrowArrayStream arrowArrayStream = CArrowArrayStreamImporter.ImportArrayStream(nativeArrayStream);

                return arrowArrayStream;
            }

            public override unsafe Schema GetTableSchema(string catalog, string db_schema, string table_name)
            {
                CArrowSchema* nativeSchema = CArrowSchema.Create();

                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionGetTableSchema, ref _nativeConnection, catalog, db_schema, table_name, nativeSchema);
                }

                Schema schema = CArrowSchemaImporter.ImportSchema(nativeSchema);

                return schema;
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
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.StatementSetSqlQuery, ref _nativeStatement, SqlQuery);

                    long rows = 0;

                    caller.Call(_nativeDriver.StatementExecuteQuery, ref _nativeStatement, null, ref rows);

                    return new UpdateResult(rows);
                }
            }

            public override object GetValue(IArrowArray arrowArray, Field field, int index)
            {
                if (arrowArray is BooleanArray)
                {
                    return Convert.ToBoolean(((BooleanArray)arrowArray).Values[index]);
                }
                else if (arrowArray is Date32Array)
                {
                    Date32Array date32Array = (Date32Array)arrowArray;

                    return date32Array.GetDateTime(index);
                }
                else if (arrowArray is Date64Array)
                {
                    Date64Array date64Array = (Date64Array)arrowArray;

                    return date64Array.GetDateTime(index);
                }
                else if (arrowArray is Decimal128Array)
                {
                    try
                    {
                        // the value may be <decimal.min or >decimal.max
                        // then Arrow throws an exception
                        // no good way to check prior to
                        return ((Decimal128Array)arrowArray).GetValue(index);
                    }
                    catch (OverflowException oex)
                    {
                        return ParseDecimalValueFromOverflowException(oex);
                    }
                }
                else if (arrowArray is Decimal256Array)
                {
                    try
                    {
                        return ((Decimal256Array)arrowArray).GetValue(index);
                    }
                    catch (OverflowException oex)
                    {
                        return ParseDecimalValueFromOverflowException(oex);
                    }
                }
                else if (arrowArray is DoubleArray)
                {
                    return ((DoubleArray)arrowArray).Values[index];
                }
                else if (arrowArray is FloatArray)
                {
                    return ((FloatArray)arrowArray).GetValue(index);
                }
#if NET5_0_OR_GREATER
                else if (arrowArray is PrimitiveArray<Half>)
                {
                    // TODO: HalfFloatArray not present in current library

                    return ((PrimitiveArray<Half>)arrowArray).GetValue(index);
                }
#endif
                else if (arrowArray is Int8Array)
                {
                    Int8Array array = (Int8Array)arrowArray;
                    return array.GetValue(index);
                }
                else if (arrowArray is Int16Array)
                {
                    return ((Int16Array)arrowArray).Values[index];
                }
                else if (arrowArray is Int32Array)
                {
                    return ((Int32Array)arrowArray).Values[index];
                }
                else if (arrowArray is Int64Array)
                {
                    Int64Array array = (Int64Array)arrowArray;

                    return array.GetValue(index);
                }
                else if (arrowArray is StringArray)
                {
                    return ((StringArray)arrowArray).GetString(index);
                }
                else if (arrowArray is Time32Array)
                {
                    return ((Time32Array)arrowArray).GetValue(index);
                }
                else if (arrowArray is Time64Array)
                {
                    return ((Time64Array)arrowArray).GetValue(index);
                }
                else if (arrowArray is TimestampArray)
                {
                    TimestampArray timestampArray = (TimestampArray)arrowArray;
                    DateTimeOffset dateTimeOffset = timestampArray.GetTimestamp(index).Value;
                    return dateTimeOffset;
                }
                else if (arrowArray is UInt8Array)
                {
                    return ((UInt8Array)arrowArray).GetValue(index);
                }
                else if (arrowArray is UInt16Array)
                {
                    return ((UInt16Array)arrowArray).GetValue(index);
                }
                else if (arrowArray is UInt32Array)
                {
                    return ((UInt32Array)arrowArray).GetValue(index);
                }
                else if (arrowArray is UInt64Array)
                {
                    return ((UInt64Array)arrowArray).GetValue(index);
                }
                else if (arrowArray is BinaryArray)
                {
                    ReadOnlySpan<byte> bytes = ((BinaryArray)arrowArray).GetBytes(index);

                    if (bytes != null)
                        return bytes.ToArray();
                }

                // not covered:
                // -- struct array
                // -- dictionary array
                // -- fixed size binary
                // -- list array
                // -- union array

                return null;
            }

            private string ParseDecimalValueFromOverflowException(OverflowException oex)
            {
                if (oex == null)
                    throw new ArgumentNullException(nameof(oex));

                // any decimal value, positive or negative, with or without a decimal in place
                Regex regex = new Regex(" -?\\d*\\.?\\d* ");

                var matches = regex.Matches(oex.Message);

                foreach (Match match in matches)
                {
                    string value = match.Value;

                    if (!string.IsNullOrEmpty(value))
                        return value;
                }

                throw oex;
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

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcDriver*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDriver nativeDriver)
            {
                fixed (CAdbcDriver* driver = &nativeDriver)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(driver, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDriver nativeDriver)
            {
                fixed (CAdbcDriver* driver = &nativeDriver)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.DriverRelease>(fn)(driver, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(db, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDatabase nativeDatabase)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.DatabaseFn>(fn)(db, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase, string key, string value)
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
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDatabase nativeDatabase, string key, string value)
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

                            TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.DatabaseSetOption>(fn)(db, (byte*)keyPtr, (byte*)valuePtr, e));
                        }
                    }
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionFn>(fn)(cn, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, string key, string value)
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
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection, string key, string value)
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

                            TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionSetOption>(fn)(cn, (byte*)keyPtr, (byte*)valuePtr, e));
                        }
                    }
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, ref CAdbcDatabase database)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcDatabase* db = &database)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, db, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection, ref CAdbcDatabase database)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcDatabase* db = &database)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionInit>(fn)(cn, db, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, stmt, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementNew>(fn)(cn, stmt, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementFn>(fn)(stmt, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement, string sqlQuery)
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
#else
            public unsafe void Call(IntPtr fn, ref CAdbcStatement nativeStatement, string sqlQuery)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper query = new Utf8Helper(sqlQuery))
                    {
                        IntPtr bQuery = (IntPtr)(query);

                        TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementSetSqlQuery>(fn)(stmt, (byte*)bQuery, e));
                    }
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement, CArrowArrayStream* arrowStream, ref long nRows)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (long* rows = &nRows)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, arrowStream, rows, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcStatement nativeStatement, CArrowArrayStream* arrowStream, ref long nRows)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (long* rows = &nRows)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementExecuteQuery>(fn)(stmt, arrowStream, rows, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeconnection, string catalog, string dbSchema, string tableName, CArrowSchema* nativeSchema)
            {
                byte* bCatalog, bDb_schema, bTable_name;

                using (Utf8Helper catalogHelper = new Utf8Helper(catalog))
                using (Utf8Helper schemaHelper = new Utf8Helper(dbSchema))
                using (Utf8Helper tableNameHelper = new Utf8Helper(tableName))
                {
                    bCatalog = (byte*)(IntPtr)(catalogHelper);
                    bDb_schema = (byte*)(IntPtr)(schemaHelper);
                    bTable_name = (byte*)(IntPtr)(tableNameHelper);

                    fixed (CAdbcConnection* connection = &nativeconnection)
                    fixed (CAdbcError* e = &_error)
                    {
                        TranslateCode(fn(connection, bCatalog, bDb_schema, bTable_name, nativeSchema, e));
                    }
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeconnection, string catalog, string dbSchema, string tableName, CArrowSchema* nativeSchema)
            {
                byte* bCatalog, bDb_schema, bTable_name;

                using (Utf8Helper catalogHelper = new Utf8Helper(catalog))
                using (Utf8Helper schemaHelper = new Utf8Helper(dbSchema))
                using (Utf8Helper tableNameHelper = new Utf8Helper(tableName))
                {
                    bCatalog = (byte*)(IntPtr)(catalogHelper);
                    bDb_schema = (byte*)(IntPtr)(schemaHelper);
                    bTable_name = (byte*)(IntPtr)(tableNameHelper);

                    fixed (CAdbcConnection* connection = &nativeconnection)
                    fixed (CAdbcError* e = &_error)
                    {
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetTableSchema>(fn)(connection, bCatalog, bDb_schema, bTable_name, nativeSchema, e));
                    }
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeconnection, CArrowArrayStream* arrowStream)
            {
                fixed (CAdbcConnection* connection = &nativeconnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(connection, arrowStream, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeconnection, CArrowArrayStream* arrowStream)
            {
                fixed (CAdbcConnection* connection = &nativeconnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetTableTypes>(fn)(connection, arrowStream, e));
                }
            }
#endif

            public unsafe void Dispose()
            {
                if (_error.release != default)
                {
                    fixed (CAdbcError* err = &_error)
                    {
#if NET5_0_OR_GREATER
                        _error.release(err);
#else
                        Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ErrorRelease>(err->release)(err);
#endif
                        _error.release = default;
                    }
                }
            }

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection connection, List<int> infoCodes, CArrowArrayStream* stream)
#else
            public unsafe void Call(IntPtr ptr, ref CAdbcConnection connection, List<int> infoCodes, CArrowArrayStream* stream)
#endif
            {
                int numInts = infoCodes.Count;

                // Calculate the total number of bytes needed
                int totalBytes = numInts * sizeof(int);

                IntPtr bytePtr = Marshal.AllocHGlobal(totalBytes);

                int[] intArray = infoCodes.ToArray();
                Marshal.Copy(intArray, 0, bytePtr, numInts);

                fixed (CAdbcConnection* cn = &connection)
                fixed (CAdbcError* e = &_error)
                {
#if NET5_0_OR_GREATER
                    TranslateCode(fn(cn, (byte*)bytePtr, infoCodes.Count, stream, e));
#else
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetInfo>(ptr)(cn, (byte*)bytePtr, infoCodes.Count, stream, e));
#endif
                }
            }

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection connection, int depth, string catalog, string db_schema, string table_name, List<string> table_types, string column_name, CArrowArrayStream* stream)
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection connection, int depth, string catalog, string db_schema, string table_name, List<string> table_types, string column_name, CArrowArrayStream* stream)
#endif
            {
                byte* bcatalog, bDb_schema, bTable_name, bColumn_Name;

                if (table_types == null)
                {
                    table_types = new List<string>();
                }

                // need to terminate with a null entry per https://github.com/apache/arrow-adbc/blob/b97e22c4d6524b60bf261e1970155500645be510/adbc.h#L909-L911
                table_types.Add(null);

                byte** bTable_type = (byte**)Marshal.AllocHGlobal(IntPtr.Size * table_types.Count);

                for (int i = 0; i < table_types.Count; i++)
                {
                    string tableType = table_types[i];
#if NETSTANDARD
                    bTable_type[i] = (byte*)MarshalExtensions.StringToCoTaskMemUTF8(tableType);
#else
                    bTable_type[i] = (byte*)Marshal.StringToCoTaskMemUTF8(tableType);
#endif
                }

                using (Utf8Helper catalogHelper = new Utf8Helper(catalog))
                using (Utf8Helper schemaHelper = new Utf8Helper(db_schema))
                using (Utf8Helper tableNameHelper = new Utf8Helper(table_name))
                using (Utf8Helper columnNameHelper = new Utf8Helper(column_name))
                {
                    bcatalog = (byte*)(IntPtr)(catalogHelper);
                    bDb_schema = (byte*)(IntPtr)(schemaHelper);
                    bTable_name = (byte*)(IntPtr)(tableNameHelper);
                    bColumn_Name = (byte*)(IntPtr)(columnNameHelper);

                    fixed (CAdbcConnection* cn = &connection)
                    fixed (CAdbcError* e = &_error)
                    {
#if NET5_0_OR_GREATER
                        TranslateCode(fn(cn, depth, bcatalog, bDb_schema, bTable_name, bTable_type, bColumn_Name, stream, e));
#else
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetObjects>(fn)(cn, depth, bcatalog, bDb_schema, bTable_name, bTable_type, bColumn_Name, stream, e));
#endif
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
