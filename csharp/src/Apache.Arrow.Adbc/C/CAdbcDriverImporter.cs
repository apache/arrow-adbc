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
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

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
        public static AdbcDriver Load(string file, string? entryPoint = null)
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

                    foreach (KeyValuePair<string, string> pair in parameters)
                    {
                        caller.Call(_nativeDriver.DatabaseSetOption, ref nativeDatabase, pair.Key, pair.Value);
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

            public unsafe override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
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
            private bool? _autoCommit;
            private IsolationLevel? _isolationLevel;
            private bool? _readOnly;

            public AdbcConnectionNative(CAdbcDriver nativeDriver, CAdbcConnection nativeConnection)
            {
                _nativeDriver = nativeDriver;
                _nativeConnection = nativeConnection;
            }

            public override bool AutoCommit
            {
                get => _autoCommit ?? throw AdbcException.NotImplemented("no value has been set for AutoCommit");
                set
                {
                    SetOption(AdbcOptions.Autocommit, AdbcOptions.GetEnabled(value));
                    _autoCommit = value;
                }
            }

            public override IsolationLevel IsolationLevel
            {
                get => _isolationLevel ?? IsolationLevel.Default;
                set
                {
                    SetOption(AdbcOptions.IsolationLevel, AdbcOptions.GetIsolationLevel(value));
                    _isolationLevel = value;
                }
            }

            public override bool ReadOnly
            {
                get => _readOnly ?? throw AdbcException.NotImplemented("no value has been set for ReadOnly");
                set
                {
                    SetOption(AdbcOptions.ReadOnly, AdbcOptions.GetEnabled(value));
                    _readOnly = value;
                }
            }

            public unsafe override AdbcStatement CreateStatement()
            {
                CAdbcStatement nativeStatement = new CAdbcStatement();

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.StatementNew
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementNew>(_nativeDriver.StatementNew)
#endif
                            (connection, &nativeStatement, &caller._error));
                    }
                }

                return new AdbcStatementNative(_nativeDriver, nativeStatement);
            }

            public unsafe override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
            {
                using (CallHelper caller = new CallHelper())
                {
                    Span<AdbcInfoCode> span = codes.AsSpan();
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    fixed (AdbcInfoCode* spanPtr = span)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.ConnectionGetInfo
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetInfo>(_nativeDriver.ConnectionGetInfo)
#endif
                            (connection, (int*)spanPtr, codes.Count, caller.CreateStream(), &caller._error));
                        return caller.ImportStream();
                    }
                }
            }

            public unsafe override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
            {
                tableTypes = tableTypes ?? [];
                byte** utf8TableTypes = null;
                try
                {
                    // need to terminate with a null entry per https://github.com/apache/arrow-adbc/blob/b97e22c4d6524b60bf261e1970155500645be510/adbc.h#L909-L911
                    utf8TableTypes = (byte**)Marshal.AllocHGlobal(IntPtr.Size * (tableTypes.Count + 1));
                    utf8TableTypes[tableTypes.Count] = null;

                    for (int i = 0; i < tableTypes.Count; i++)
                    {
                        string tableType = tableTypes[i];
                        utf8TableTypes[i] = (byte*)MarshalExtensions.StringToCoTaskMemUTF8(tableType);
                    }

                    using (Utf8Helper utf8Catalog = new Utf8Helper(catalogPattern))
                    using (Utf8Helper utf8Schema = new Utf8Helper(dbSchemaPattern))
                    using (Utf8Helper utf8Table = new Utf8Helper(tableNamePattern))
                    using (Utf8Helper utf8Column = new Utf8Helper(columnNamePattern))
                    using (CallHelper caller = new CallHelper())
                    {
                        fixed (CAdbcConnection* connection = &_nativeConnection)
                        {
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                _nativeDriver.ConnectionGetObjects
#else
                                Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetObjects>(_nativeDriver.ConnectionGetObjects)
#endif
                                (connection, (int)depth, utf8Catalog, utf8Schema, utf8Table, utf8TableTypes, utf8Column, caller.CreateStream(), &caller._error));
                            return caller.ImportStream();
                        }
                    }
                }
                finally
                {
                    for (int i = 0; i < tableTypes.Count; i++)
                    {
                        Marshal.FreeCoTaskMem((IntPtr)utf8TableTypes[i]);
                    }
                    Marshal.FreeHGlobal((IntPtr)utf8TableTypes);
                }
            }

            public unsafe override IArrowArrayStream GetTableTypes()
            {
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.ConnectionGetTableTypes
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetTableTypes>(_nativeDriver.ConnectionGetTableTypes)
#endif
                            (connection, caller.CreateStream(), &caller._error));
                        return caller.ImportStream();
                    }
                }
            }

            public unsafe override Schema GetTableSchema(string? catalog, string? db_schema, string table_name)
            {
                using (Utf8Helper utf8Catalog = new Utf8Helper(catalog))
                using (Utf8Helper utf8Schema = new Utf8Helper(db_schema))
                using (Utf8Helper utf8Table = new Utf8Helper(table_name))
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.ConnectionGetTableSchema
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionGetTableSchema>(_nativeDriver.ConnectionGetTableSchema)
#endif
                            (connection, utf8Catalog, utf8Schema, utf8Table, caller.CreateSchema(), &caller._error));
                        return caller.ImportSchema();
                    }
                }
            }

            public unsafe override void Commit()
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionCommit, ref _nativeConnection);
                }
            }

            public unsafe override void Rollback()
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionRollback, ref _nativeConnection);
                }
            }

            public unsafe override void SetOption(string key, string value)
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(_nativeDriver.ConnectionSetOption, ref _nativeConnection, key, value);
                }
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcStatement"/>
        /// </summary>
        sealed class AdbcStatementNative : AdbcStatement
        {
            private CAdbcDriver _nativeDriver;
            private CAdbcStatement _nativeStatement;
            private byte[]? _substraitPlan;

            public AdbcStatementNative(CAdbcDriver nativeDriver, CAdbcStatement nativeStatement)
            {
                _nativeDriver = nativeDriver;
                _nativeStatement = nativeStatement;
            }

            public unsafe override byte[]? SubstraitPlan
            {
                get => _substraitPlan;
                set
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        fixed (CAdbcStatement* statement = &_nativeStatement)
                        fixed (byte* substraitPlan = value)
                        {
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                _nativeDriver.StatementSetSubstraitPlan
#else
                                Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementSetSubstraitPlan>(_nativeDriver.StatementSetSubstraitPlan)
#endif
                                (statement, substraitPlan, value?.Length ?? 0, &caller._error));
                        }
                        _substraitPlan = value;
                    }
                }
            }

            public unsafe override void Bind(RecordBatch batch, Schema schema)
            {
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        CArrowArrayExporter.ExportRecordBatch(batch, caller.CreateArray());
                        CArrowSchemaExporter.ExportSchema(schema, caller.CreateSchema());

                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.StatementBind
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementBind>(_nativeDriver.StatementBind)
#endif
                            (statement, caller.Array, caller.Schema, &caller._error));

                        // On success, ownership passes to the driver
                        caller.ForgetArray();
                        caller.ForgetSchema();
                    }
                }
            }

            public unsafe override void BindStream(IArrowArrayStream stream)
            {
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        CArrowArrayStreamExporter.ExportArrayStream(stream, caller.CreateStream());
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.StatementBindStream
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementBindStream>(_nativeDriver.StatementBindStream)
#endif
                            (statement, caller.ArrayStream, &caller._error));

                        // On success, ownership passes to the driver
                        caller.ForgetStream();
                    }
                }
            }

            public unsafe override QueryResult ExecuteQuery()
            {
                if (SqlQuery != null)
                {
                    // TODO: Consider moving this to the setter
                    SetSqlQuery(SqlQuery);
                }

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        long rows = 0;
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.StatementExecuteQuery
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementExecuteQuery>(_nativeDriver.StatementExecuteQuery)
#endif
                            (statement, caller.CreateStream(), &rows, &caller._error));

                        return new QueryResult(rows, caller.ImportStream());
                    }
                }
            }

            public unsafe override UpdateResult ExecuteUpdate()
            {
                if (SqlQuery != null)
                {
                    // TODO: Consider moving this to the setter
                    SetSqlQuery(SqlQuery);
                }

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        long rows = 0;
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.StatementExecuteQuery
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementExecuteQuery>(_nativeDriver.StatementExecuteQuery)
#endif
                            (statement, caller.CreateStream(), &rows, &caller._error));

                        return new UpdateResult(rows);
                    }
                }
            }

            public unsafe override PartitionedResult ExecutePartitioned()
            {
                if (SqlQuery != null)
                {
                    // TODO: Consider moving this to the setter
                    SetSqlQuery(SqlQuery);
                }

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        CAdbcPartitions* nativePartitions = null;
                        long rowsAffected = 0;
                        try
                        {
                            nativePartitions = (CAdbcPartitions*)Marshal.AllocHGlobal(sizeof(CAdbcPartitions));
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                _nativeDriver.StatementExecutePartitions
#else
                                Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementExecutePartitions>(_nativeDriver.StatementExecutePartitions)
#endif
                                (statement, caller.CreateSchema(), nativePartitions, &rowsAffected, &caller._error));

                            PartitionDescriptor[] partitions = new PartitionDescriptor[nativePartitions->num_partitions];
                            for (int i = 0; i < partitions.Length; i++)
                            {
                                partitions[i] = new PartitionDescriptor(MarshalExtensions.MarshalBuffer(nativePartitions->partitions[i], checked((int)nativePartitions->partition_lengths[i]))!);
                            }

                            return new PartitionedResult(caller.ImportSchema(), rowsAffected, partitions);
                        }
                        finally
                        {
                            if (nativePartitions->release != null)
                            {
#if NET5_0_OR_GREATER
                                nativePartitions->release(nativePartitions);
#else
                                Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.PartitionsRelease>(nativePartitions->release)(nativePartitions);
#endif
                            }

                            if (nativePartitions != null)
                            {
                                Marshal.FreeHGlobal((IntPtr)nativePartitions);
                            }
                        }
                    }
                }
            }

            private unsafe void SetSqlQuery(string sqlQuery)
            {
                fixed (CAdbcStatement* statement = &_nativeStatement)
                {
                    using (Utf8Helper query = new Utf8Helper(sqlQuery))
                    using (CallHelper caller = new CallHelper())
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _nativeDriver.StatementSetSqlQuery
#else
                            Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.StatementSetSqlQuery>(_nativeDriver.StatementSetSqlQuery)
#endif
                            (statement, query, &caller._error));
                    }
                }
            }
        }

        /// <summary>
        /// Assists with UTF8/string marshalling
        /// </summary>
        private struct Utf8Helper : IDisposable
        {
            private IntPtr _s;

            public Utf8Helper(string? s)
            {
                if (s != null) _s = MarshalExtensions.StringToCoTaskMemUTF8(s);
            }

            public static implicit operator IntPtr(Utf8Helper s) { return s._s; }
            public static unsafe implicit operator byte*(Utf8Helper s) { return (byte*)s._s; }

            public void Dispose()
            {
                if (_s != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(_s);
                    _s = IntPtr.Zero;
                }
            }
        }

        /// <summary>
        /// Assists with delegate calls and handling error codes
        /// </summary>
        private unsafe struct CallHelper : IDisposable
        {
            public CAdbcError _error;
            private CArrowSchema* _schema;
            private CArrowArray* _array;
            private CArrowArrayStream* _arrayStream;

            public CArrowSchema* Schema => _schema;
            public CArrowArray* Array => _array;
            public CArrowArrayStream* ArrayStream => _arrayStream;

            public CArrowSchema* CreateSchema()
            {
                Debug.Assert(_schema == null);
                _schema = CArrowSchema.Create();
                return _schema;
            }

            public void ForgetSchema()
            {
                Debug.Assert(_schema != null);
                _schema = null;
            }

            public Schema ImportSchema()
            {
                Debug.Assert(_schema != null);
                Schema schema = CArrowSchemaImporter.ImportSchema(_schema);
                _schema = null;
                return schema;
            }

            public CArrowArray* CreateArray()
            {
                Debug.Assert(_array == null);
                _array = CArrowArray.Create();
                return _array;
            }

            public void ForgetArray()
            {
                Debug.Assert(_array != null);
                _array = null;
            }

            public CArrowArrayStream* CreateStream()
            {
                Debug.Assert(_arrayStream == null);
                _arrayStream = CArrowArrayStream.Create();
                return _arrayStream;
            }

            public void ForgetStream()
            {
                Debug.Assert(_arrayStream != null);
                _arrayStream = null;
            }

            public IArrowArrayStream ImportStream()
            {
                Debug.Assert(_arrayStream != null);
                IArrowArrayStream arrayStream = CArrowArrayStreamImporter.ImportArrayStream(_arrayStream);
                _arrayStream = null;
                return arrayStream;
            }

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
            public unsafe void Call(delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase, string key, string? value)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(fn(db, utf8Key, utf8Value, e));
                    }
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDatabase nativeDatabase, string key, string? value)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.DatabaseSetOption>(fn)(db, utf8Key, utf8Value, e));
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
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, string key, string? value)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(fn(cn, utf8Key, utf8Value, e));
                    }
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection, string key, string? value)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<CAdbcDriverExporter.ConnectionSetOption>(fn)(cn, utf8Key, utf8Value, e));
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

                if (_array != null)
                {
                    CArrowArray.Free(_array);
                    _array = null;
                }
                if (_schema != null)
                {
                    CArrowSchema.Free(_schema);
                    _schema = null;
                }
                if (_arrayStream != null)
                {
                    CArrowArrayStream.Free(_arrayStream);
                    _arrayStream = null;
                }
            }

            public unsafe void TranslateCode(AdbcStatusCode statusCode)
            {
                if (statusCode != AdbcStatusCode.Success)
                {
                    string message = "Undefined error";
                    if (_error.message != null)
                    {
                        message = MarshalExtensions.PtrToStringUTF8(_error.message)!;
                    }

                    Dispose();

                    throw new AdbcException(message);
                }
            }
        }
    }
}
