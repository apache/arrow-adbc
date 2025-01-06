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
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.C
{
    public class CAdbcDriverExporter
    {
        private static unsafe readonly NativeDelegate<ErrorRelease> s_releaseError = new NativeDelegate<ErrorRelease>(ReleaseError);

#if NET5_0_OR_GREATER
        internal static unsafe delegate* unmanaged<CAdbcError*, void> ReleaseErrorPtr => (delegate* unmanaged<CAdbcError*, void>)s_releaseError.Pointer;
        private static unsafe delegate* unmanaged<CAdbcDriver*, CAdbcError*, AdbcStatusCode> ReleaseDriverPtr => &ReleaseDriver;
        private static unsafe delegate* unmanaged<CAdbcPartitions*, void> ReleasePartitionsPtr => &ReleasePartitions;

        private static unsafe delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseInitPtr => &InitDatabase;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseReleasePtr => &ReleaseDatabase;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> DatabaseSetOptionPtr => &SetDatabaseOption;

        private static unsafe delegate* unmanaged<CAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetObjectsPtr => &GetConnectionObjects;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, CAdbcError*, AdbcStatusCode> ConnectionGetTableSchemaPtr => &GetConnectionTableSchema;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetTableTypesPtr => &GetConnectionTableTypes;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode> ConnectionInitPtr => &InitConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionRollbackPtr => &RollbackConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCommitPtr => &CommitConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionReleasePtr => &ReleaseConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, int*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetInfoPtr => &GetConnectionInfo;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionReadPartitionPtr => &ReadConnectionPartition;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> ConnectionSetOptionPtr => &SetConnectionOption;

        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArray*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementBindPtr => &BindStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> StatementBindStreamPtr => &BindStreamStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode> StatementExecuteQueryPtr => &ExecuteStatementQuery;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcPartitions*, long*, CAdbcError*, AdbcStatusCode> StatementExecutePartitionsPtr => &ExecuteStatementPartitions;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementExecuteSchemaPtr => &ExecuteStatementSchema;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementNewPtr => &NewStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementReleasePtr => &ReleaseStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementPreparePtr => &PrepareStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> StatementSetSqlQueryPtr => &SetStatementSqlQuery;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, int, CAdbcError*, AdbcStatusCode> StatementSetSubstraitPlanPtr => &SetStatementSubstraitPlan;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementGetParameterSchemaPtr => &GetStatementParameterSchema;
#else
        internal static unsafe IntPtr ReleaseErrorPtr => s_releaseError.Pointer;
        private static unsafe IntPtr ReleaseDriverPtr = NativeDelegate<DriverRelease>.AsNativePointer(ReleaseDriver);
        private static unsafe IntPtr ReleasePartitionsPtr = NativeDelegate<PartitionsRelease>.AsNativePointer(ReleasePartitions);

        private static unsafe IntPtr DatabaseInitPtr = NativeDelegate<DatabaseFn>.AsNativePointer(InitDatabase);
        private static unsafe IntPtr DatabaseReleasePtr = NativeDelegate<DatabaseFn>.AsNativePointer(ReleaseDatabase);
        private static unsafe IntPtr DatabaseSetOptionPtr = NativeDelegate<DatabaseSetOption>.AsNativePointer(SetDatabaseOption);

        private static unsafe IntPtr ConnectionGetObjectsPtr = NativeDelegate<ConnectionGetObjects>.AsNativePointer(GetConnectionObjects);
        private static unsafe IntPtr ConnectionGetTableSchemaPtr = NativeDelegate<ConnectionGetTableSchema>.AsNativePointer(GetConnectionTableSchema);
        private static unsafe IntPtr ConnectionGetTableTypesPtr = NativeDelegate<ConnectionGetTableTypes>.AsNativePointer(GetConnectionTableTypes);
        private static unsafe IntPtr ConnectionInitPtr = NativeDelegate<ConnectionInit>.AsNativePointer(InitConnection);
        private static unsafe IntPtr ConnectionRollbackPtr = NativeDelegate<ConnectionFn>.AsNativePointer(RollbackConnection);
        private static unsafe IntPtr ConnectionCommitPtr = NativeDelegate<ConnectionFn>.AsNativePointer(CommitConnection);
        private static unsafe IntPtr ConnectionReleasePtr = NativeDelegate<ConnectionFn>.AsNativePointer(ReleaseConnection);
        private static unsafe IntPtr ConnectionGetInfoPtr = NativeDelegate<ConnectionGetInfo>.AsNativePointer(GetConnectionInfo);
        private static unsafe IntPtr ConnectionReadPartitionPtr = NativeDelegate<ConnectionReadPartition>.AsNativePointer(ReadConnectionPartition);
        private static unsafe IntPtr ConnectionSetOptionPtr = NativeDelegate<ConnectionSetOption>.AsNativePointer(SetConnectionOption);

        private static unsafe IntPtr StatementBindPtr = NativeDelegate<StatementBind>.AsNativePointer(BindStatement);
        private static unsafe IntPtr StatementBindStreamPtr = NativeDelegate<StatementBindStream>.AsNativePointer(BindStreamStatement);
        private static unsafe IntPtr StatementExecuteQueryPtr = NativeDelegate<StatementExecuteQuery>.AsNativePointer(ExecuteStatementQuery);
        private static unsafe IntPtr StatementExecutePartitionsPtr = NativeDelegate<StatementExecutePartitions>.AsNativePointer(ExecuteStatementPartitions);
        private static unsafe IntPtr StatementExecuteSchemaPtr = NativeDelegate<StatementExecuteSchema>.AsNativePointer(ExecuteStatementSchema);
        private static unsafe IntPtr StatementNewPtr = NativeDelegate<StatementNew>.AsNativePointer(NewStatement);
        private static unsafe IntPtr StatementReleasePtr = NativeDelegate<StatementRelease>.AsNativePointer(ReleaseStatement);
        private static unsafe IntPtr StatementPreparePtr = NativeDelegate<StatementPrepare>.AsNativePointer(PrepareStatement);
        private static unsafe IntPtr StatementSetSqlQueryPtr = NativeDelegate<StatementSetSqlQuery>.AsNativePointer(SetStatementSqlQuery);
        private static unsafe IntPtr StatementSetSubstraitPlanPtr = NativeDelegate<StatementSetSubstraitPlan>.AsNativePointer(SetStatementSubstraitPlan);
        private static unsafe IntPtr StatementGetParameterSchemaPtr = NativeDelegate<StatementGetParameterSchema>.AsNativePointer(GetStatementParameterSchema);
#endif

        public unsafe static AdbcStatusCode AdbcDriverInit(int version, CAdbcDriver* nativeDriver, CAdbcError* error, AdbcDriver driver)
        {
            if (version != AdbcVersion.Version_1_0_0)
            {
                // TODO: implement support for AdbcVersion.Version_1_1_0
                return AdbcStatusCode.InternalError;
            }

            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDriver->private_data = (void*)GCHandle.ToIntPtr(handle);
            nativeDriver->release = ReleaseDriverPtr;

            nativeDriver->DatabaseInit = DatabaseInitPtr;
            nativeDriver->DatabaseNew = stub.NewDatabasePtr;
            nativeDriver->DatabaseSetOption = DatabaseSetOptionPtr;
            nativeDriver->DatabaseRelease = DatabaseReleasePtr;

            nativeDriver->ConnectionCommit = ConnectionCommitPtr;
            nativeDriver->ConnectionGetInfo = ConnectionGetInfoPtr;
            nativeDriver->ConnectionGetObjects = ConnectionGetObjectsPtr;
            nativeDriver->ConnectionGetTableSchema = ConnectionGetTableSchemaPtr;
            nativeDriver->ConnectionGetTableTypes = ConnectionGetTableTypesPtr;
            nativeDriver->ConnectionInit = ConnectionInitPtr;
            nativeDriver->ConnectionNew = stub.NewConnectionPtr;
            nativeDriver->ConnectionSetOption = ConnectionSetOptionPtr;
            nativeDriver->ConnectionReadPartition = ConnectionReadPartitionPtr;
            nativeDriver->ConnectionRelease = ConnectionReleasePtr;
            nativeDriver->ConnectionRollback = ConnectionRollbackPtr;

            nativeDriver->StatementBind = StatementBindPtr;
            nativeDriver->StatementBindStream = StatementBindStreamPtr;
            nativeDriver->StatementExecuteQuery = StatementExecuteQueryPtr;
            nativeDriver->StatementExecutePartitions = StatementExecutePartitionsPtr;
            nativeDriver->StatementGetParameterSchema = StatementGetParameterSchemaPtr;
            nativeDriver->StatementNew = StatementNewPtr;
            nativeDriver->StatementPrepare = StatementPreparePtr;
            nativeDriver->StatementRelease = StatementReleasePtr;
            nativeDriver->StatementSetSqlQuery = StatementSetSqlQueryPtr;
            nativeDriver->StatementSetSubstraitPlan = StatementSetSubstraitPlanPtr;

            return 0;
        }

        internal unsafe static void ReleaseError(CAdbcError* error)
        {
            if (error != null && ((IntPtr)error->message) != IntPtr.Zero)
            {
                Marshal.FreeHGlobal((IntPtr)error->message);
            }
        }

        private unsafe static AdbcStatusCode SetError(CAdbcError* error, Exception exception)
        {
            ReleaseError(error);

            error->message = (byte*)MarshalExtensions.StringToCoTaskMemUTF8(exception.Message);
            error->sqlstate0 = (byte)0;
            error->sqlstate1 = (byte)0;
            error->sqlstate2 = (byte)0;
            error->sqlstate3 = (byte)0;
            error->sqlstate4 = (byte)0;
            error->vendor_code = 0;
            error->release = ReleaseErrorPtr;

            if (exception is AdbcException adbcException)
            {
                if (adbcException.SqlState != null)
                {
                    byte* dest = &error->sqlstate0;
                    fixed (char* sqlState = adbcException.SqlState)
                    {
                        int len = Math.Min(5, adbcException.SqlState.Length);
                        for (int i = 0; i < len; i++)
                        {
                            dest[i] = unchecked((byte)sqlState[i]);
                        }
                    }
                }
                return adbcException.Status;
            }

            return AdbcStatusCode.UnknownError;
        }

        private static IntPtr FromDisposable(IDisposable d)
        {
            GCHandle gch = GCHandle.Alloc(d);
            return GCHandle.ToIntPtr(gch);
        }

        private static void Dispose(ref IntPtr p)
        {
            GCHandle gch = GCHandle.FromIntPtr(p);
            ((IDisposable)gch.Target!).Dispose();
            gch.Free();
            p = IntPtr.Zero;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseDriver(CAdbcDriver* nativeDriver, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDriver->private_data);
                DriverStub stub = (DriverStub)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeDriver->private_data = null;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void ReleasePartitions(CAdbcPartitions* partitions)
        {
            if (partitions != null)
            {
                if (partitions->partitions != null)
                {
                    for (int i = 0; i < partitions->num_partitions; i++)
                    {
                        byte* partition = partitions->partitions[i];
                        if (partition != null)
                        {
                            Marshal.FreeHGlobal((IntPtr)partition);
                            partitions->partitions[i] = null;
                        }
                    }
                    Marshal.FreeHGlobal((IntPtr)partitions->partitions);
                    partitions->partitions = null;
                }
                if (partitions->partition_lengths != null)
                {
                    Marshal.FreeHGlobal((IntPtr)partitions->partition_lengths);
                    partitions->partition_lengths = null;
                }

                partitions->release = default;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode InitDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;
                stub.Init();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeDatabase->private_data = null;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetConnectionOption(CAdbcConnection* nativeConnection, byte* name, byte* value, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.SetOption(name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetDatabaseOption(CAdbcDatabase* nativeDatabase, byte* name, byte* value, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;
                stub.SetOption(name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode InitConnection(CAdbcConnection* nativeConnection, CAdbcDatabase* database, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.InitConnection(ref *database);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }


#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionObjects(CAdbcConnection* nativeConnection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetObjects(depth, catalog, db_schema, table_name, table_type, column_name, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionTableTypes(CAdbcConnection* nativeConnection, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetTableTypes(stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionTableSchema(CAdbcConnection* nativeConnection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetTableSchema(catalog, db_schema, table_name, schema);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode RollbackConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Rollback();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode CommitConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Commit();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeConnection->private_data = null;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReadConnectionPartition(CAdbcConnection* nativeConnection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.ReadPartition(serialized_partition, serialized_length, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionInfo(CAdbcConnection* nativeConnection, int* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetInfo(info_codes, info_codes_length, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementSqlQuery(CAdbcStatement* nativeStatement, byte* text, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                stub.SqlQuery = MarshalExtensions.PtrToStringUTF8(text);

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementSubstraitPlan(CAdbcStatement* nativeStatement, byte* plan, int length, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                stub.SubstraitPlan = MarshalExtensions.MarshalBuffer(plan, length);

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetStatementParameterSchema(CAdbcStatement* nativeStatement, CArrowSchema* schema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                CArrowSchemaExporter.ExportSchema(stub.GetParameterSchema(), schema);

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode BindStatement(CAdbcStatement* nativeStatement, CArrowArray* array, CArrowSchema* cschema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                Schema schema = CArrowSchemaImporter.ImportSchema(cschema);
                RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(array, schema);
                stub.Bind(batch, schema);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode BindStreamStatement(CAdbcStatement* nativeStatement, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                IArrowArrayStream arrayStream = CArrowArrayStreamImporter.ImportArrayStream(stream);
                stub.BindStream(arrayStream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementQuery(CAdbcStatement* nativeStatement, CArrowArrayStream* stream, long* rows, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                var result = stub.ExecuteQuery();
                if (rows != null)
                {
                    *rows = result.RowCount;
                }

                CArrowArrayStreamExporter.ExportArrayStream(result.Stream, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementPartitions(CAdbcStatement* nativeStatement, CArrowSchema* schema, CAdbcPartitions* partitions, long* rows, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                var result = stub.ExecutePartitioned();
                if (rows != null)
                {
                    *rows = result.AffectedRows;
                }

                partitions->release = ReleasePartitionsPtr;
                partitions->num_partitions = result.PartitionDescriptors.Count;
                partitions->partitions = (byte**)Marshal.AllocHGlobal(IntPtr.Size * result.PartitionDescriptors.Count);
                partitions->partition_lengths = (nuint*)Marshal.AllocHGlobal(IntPtr.Size * result.PartitionDescriptors.Count);
                for (int i = 0; i < partitions->num_partitions; i++)
                {
                    ReadOnlySpan<byte> partition = result.PartitionDescriptors[i].Descriptor;
                    partitions->partition_lengths[i] = (nuint)partition.Length;
                    partitions->partitions[i] = (byte*)Marshal.AllocHGlobal(partition.Length);
                    fixed (void* descriptor = partition)
                    {
                        Buffer.MemoryCopy(descriptor, partitions->partitions[i], partition.Length, partition.Length);
                    }
                }

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementSchema(CAdbcStatement* nativeStatement, CArrowSchema* schema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                var result = stub.ExecuteSchema();

                CArrowSchemaExporter.ExportSchema(result, schema);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode NewStatement(CAdbcConnection* nativeConnection, CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.NewStatement(ref *nativeStatement);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeStatement->private_data = null;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode PrepareStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement statement = (AdbcStatement)gch.Target!;
                statement.Prepare();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

        private sealed class DriverStub : IDisposable
        {
            private readonly AdbcDriver _driver;
            private unsafe readonly NativeDelegate<DatabaseFn> newDatabase;
            private unsafe readonly NativeDelegate<ConnectionFn> newConnection;

#if NET5_0_OR_GREATER
            internal unsafe delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> NewDatabasePtr =>
                (delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode>)newDatabase.Pointer;
            internal unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> NewConnectionPtr =>
                (delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode>)newConnection.Pointer;
#else
            internal IntPtr NewDatabasePtr => newDatabase.Pointer;
            internal IntPtr NewConnectionPtr => newConnection.Pointer;
#endif

            public DriverStub(AdbcDriver driver)
            {
                _driver = driver;

                unsafe
                {
                    newDatabase = new NativeDelegate<DatabaseFn>(NewDatabase);
                    newConnection = new NativeDelegate<ConnectionFn>(NewConnection);
                }
            }

            private unsafe AdbcStatusCode NewDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
            {
                try
                {
                    DatabaseStub stub = new DatabaseStub(_driver);
                    GCHandle handle = GCHandle.Alloc(stub);
                    nativeDatabase->private_data = (void*)GCHandle.ToIntPtr(handle);
                    return AdbcStatusCode.Success;
                }
                catch (Exception e)
                {
                    return SetError(error, e);
                }
            }

            private unsafe AdbcStatusCode NewConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
            {
                try
                {
                    ConnectionStub stub = new ConnectionStub(_driver);
                    GCHandle handle = GCHandle.Alloc(stub);
                    nativeConnection->private_data = (void*)GCHandle.ToIntPtr(handle);
                    return AdbcStatusCode.Success;
                }
                catch (Exception e)
                {
                    return SetError(error, e);
                }
            }

            public void Dispose()
            {
                _driver.Dispose();
            }
        }

        sealed class DatabaseStub : IDisposable
        {
            readonly AdbcDriver _driver;
            readonly Dictionary<string, string> options;
            AdbcDatabase? database;

            public DatabaseStub(AdbcDriver driver)
            {
                _driver = driver;
                options = new Dictionary<string, string>();
            }

            public void Init()
            {
                if (database != null) throw new InvalidOperationException("database already initialized");

                database = _driver.Open(options);
            }

            public unsafe void SetOption(byte* name, byte* value)
            {
                if (name == null) throw new ArgumentNullException(nameof(name));
                if (value == null) throw new ArgumentNullException(nameof(value));

                options[MarshalExtensions.PtrToStringUTF8(name)!] = MarshalExtensions.PtrToStringUTF8(value)!;
            }

            public void OpenConnection(IReadOnlyDictionary<string, string> options, out AdbcConnection connection)
            {
                if (database == null) throw new InvalidOperationException("database not initialized");
                connection = database.Connect(options);
            }

            public void Dispose()
            {
                database?.Dispose();
                database = null;
            }
        }

        sealed class ConnectionStub : IDisposable
        {
            readonly AdbcDriver _driver;
            readonly Dictionary<string, string> options;
            AdbcConnection? connection;

            public ConnectionStub(AdbcDriver driver)
            {
                _driver = driver;
                options = new Dictionary<string, string>();
            }

            internal AdbcConnection Connection => connection == null ? throw new InvalidOperationException("connection is not open") : connection;

            public unsafe void SetOption(byte* name, byte* value)
            {
                if (name == null) throw new ArgumentNullException(nameof(name));
                if (value == null) throw new ArgumentNullException(nameof(value));

                string stringName = MarshalExtensions.PtrToStringUTF8(name)!;
                string stringValue = MarshalExtensions.PtrToStringUTF8(value)!;

                if (connection == null)
                {
                    options[stringName] = stringValue;
                }
                else
                {
                    switch (stringName)
                    {
                        case AdbcOptions.Connection.Autocommit:
                            connection.AutoCommit = AdbcOptions.GetEnabled(stringValue);
                            break;
                        case AdbcOptions.Connection.IsolationLevel:
                            connection.IsolationLevel = AdbcOptions.GetIsolationLevel(stringValue);
                            break;
                        case AdbcOptions.Connection.ReadOnly:
                            connection.ReadOnly = AdbcOptions.GetEnabled(stringValue);
                            break;
                        default:
                            connection.SetOption(stringName, stringValue);
                            break;
                    }
                }
            }

            public void Rollback() { Connection.Rollback(); }
            public void Commit() { Connection.Commit(); }

            public void Dispose()
            {
                connection?.Dispose();
                connection = null;
            }

            public unsafe void GetObjects(int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* cstream)
            {
                string? catalogPattern = MarshalExtensions.PtrToStringUTF8(catalog);
                string? dbSchemaPattern = MarshalExtensions.PtrToStringUTF8(db_schema);
                string? tableNamePattern = MarshalExtensions.PtrToStringUTF8(table_name);
                string? columnNamePattern = MarshalExtensions.PtrToStringUTF8(column_name);

                string[]? tableTypes = null;
                const int maxTableTypeCount = 100;
                if (table_type != null)
                {
                    int count = 0;
                    while (table_type[count] != null && count <= maxTableTypeCount)
                    {
                        count++;
                    }

                    if (count > maxTableTypeCount)
                    {
                        throw new InvalidOperationException($"We do not expect to get more than {maxTableTypeCount} table types");
                    }

                    tableTypes = new string[count];
                    for (int i = 0; i < count; i++)
                    {
                        tableTypes[i] = MarshalExtensions.PtrToStringUTF8((IntPtr)table_type[i])!;
                    }
                }

                AdbcConnection.GetObjectsDepth goDepth = (AdbcConnection.GetObjectsDepth)depth;

                IArrowArrayStream stream = Connection.GetObjects(goDepth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern);

                CArrowArrayStreamExporter.ExportArrayStream(stream, cstream);
            }

            public unsafe void GetTableSchema(byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* cschema)
            {
                string? sCatalog = MarshalExtensions.PtrToStringUTF8(catalog);
                string? sDbSchema = MarshalExtensions.PtrToStringUTF8(db_schema);
                string sTableName = MarshalExtensions.PtrToStringUTF8(table_name)!;

                Schema schema = Connection.GetTableSchema(sCatalog, sDbSchema, sTableName);

                CArrowSchemaExporter.ExportSchema(schema, cschema);
            }

            public unsafe void GetTableTypes(CArrowArrayStream* cArrayStream)
            {
                CArrowArrayStreamExporter.ExportArrayStream(Connection.GetTableTypes(), cArrayStream);
            }

            public unsafe void ReadPartition(byte* serializedPartition, int serialized_length, CArrowArrayStream* stream)
            {
                byte[] partition = new byte[serialized_length];
                fixed (byte* partitionPtr = partition)
                {
                    Buffer.MemoryCopy(serializedPartition, partitionPtr, serialized_length, serialized_length);
                }

                CArrowArrayStreamExporter.ExportArrayStream(Connection.ReadPartition(new PartitionDescriptor(partition)), stream);
            }

            public unsafe void GetInfo(int* info_codes, int info_codes_length, CArrowArrayStream* stream)
            {
                AdbcInfoCode[] infoCodes = new AdbcInfoCode[info_codes_length];
                fixed (AdbcInfoCode* infoCodesPtr = infoCodes)
                {
                    long length = (long)info_codes_length * sizeof(int);
                    Buffer.MemoryCopy(info_codes, infoCodesPtr, length, length);
                }

                CArrowArrayStreamExporter.ExportArrayStream(Connection.GetInfo(infoCodes.ToList()), stream);
            }

            public unsafe void InitConnection(ref CAdbcDatabase nativeDatabase)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;
                stub.OpenConnection(options, out connection);
            }

            public unsafe void NewStatement(ref CAdbcStatement nativeStatement)
            {
                AdbcStatement statement = Connection.CreateStatement();
                GCHandle handle = GCHandle.Alloc(statement);
                nativeStatement.private_data = (void*)GCHandle.ToIntPtr(handle);
            }
        }
    }
}
