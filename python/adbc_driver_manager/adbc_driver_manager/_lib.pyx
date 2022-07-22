# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

"""Low-level ADBC API."""

import enum
import typing
from typing import List

import cython
from libc.stdint cimport int32_t, uint8_t, uint32_t, uintptr_t
from libc.string cimport memset
from libcpp.vector cimport vector as c_vector

if typing.TYPE_CHECKING:
    from typing import Self


cdef extern from "adbc.h" nogil:
    # C ABI
    cdef struct CArrowSchema"ArrowSchema":
        pass
    cdef struct CArrowArray"ArrowArray":
        pass
    cdef struct CArrowArrayStream"ArrowArrayStream":
        pass

    # ADBC
    ctypedef uint8_t CAdbcStatusCode"AdbcStatusCode"
    cdef CAdbcStatusCode ADBC_STATUS_OK
    cdef CAdbcStatusCode ADBC_STATUS_UNKNOWN
    cdef CAdbcStatusCode ADBC_STATUS_NOT_IMPLEMENTED
    cdef CAdbcStatusCode ADBC_STATUS_NOT_FOUND
    cdef CAdbcStatusCode ADBC_STATUS_ALREADY_EXISTS
    cdef CAdbcStatusCode ADBC_STATUS_INVALID_ARGUMENT
    cdef CAdbcStatusCode ADBC_STATUS_INVALID_STATE
    cdef CAdbcStatusCode ADBC_STATUS_INVALID_DATA
    cdef CAdbcStatusCode ADBC_STATUS_INTEGRITY
    cdef CAdbcStatusCode ADBC_STATUS_INTERNAL
    cdef CAdbcStatusCode ADBC_STATUS_IO
    cdef CAdbcStatusCode ADBC_STATUS_CANCELLED
    cdef CAdbcStatusCode ADBC_STATUS_TIMEOUT
    cdef CAdbcStatusCode ADBC_STATUS_UNAUTHENTICATED
    cdef CAdbcStatusCode ADBC_STATUS_UNAUTHORIZED

    cdef const char* ADBC_OPTION_VALUE_DISABLED
    cdef const char* ADBC_OPTION_VALUE_ENABLED

    cdef const char* ADBC_CONNECTION_OPTION_AUTOCOMMIT
    cdef const char* ADBC_INGEST_OPTION_TARGET_TABLE

    cdef int ADBC_OBJECT_DEPTH_ALL
    cdef int ADBC_OBJECT_DEPTH_CATALOGS
    cdef int ADBC_OBJECT_DEPTH_DB_SCHEMAS
    cdef int ADBC_OBJECT_DEPTH_TABLES
    cdef int ADBC_OBJECT_DEPTH_COLUMNS

    cdef uint32_t ADBC_INFO_VENDOR_NAME
    cdef uint32_t ADBC_INFO_VENDOR_VERSION
    cdef uint32_t ADBC_INFO_VENDOR_ARROW_VERSION
    cdef uint32_t ADBC_INFO_DRIVER_NAME
    cdef uint32_t ADBC_INFO_DRIVER_VERSION
    cdef uint32_t ADBC_INFO_DRIVER_ARROW_VERSION

    ctypedef void (*CAdbcErrorRelease)(CAdbcError*)

    cdef struct CAdbcError"AdbcError":
        char* message
        int32_t vendor_code
        char[5] sqlstate
        CAdbcErrorRelease release

    cdef struct CAdbcDatabase"AdbcDatabase":
        void* private_data

    cdef struct CAdbcConnection"AdbcConnection":
        void* private_data

    cdef struct CAdbcStatement"AdbcStatement":
        void* private_data

    CAdbcStatusCode AdbcDatabaseNew(CAdbcDatabase* database, CAdbcError* error)
    CAdbcStatusCode AdbcDatabaseSetOption(
        CAdbcDatabase* database,
        const char* key,
        const char* value,
        CAdbcError* error)
    CAdbcStatusCode AdbcDatabaseInit(CAdbcDatabase* database, CAdbcError* error)
    CAdbcStatusCode AdbcDatabaseRelease(CAdbcDatabase* database, CAdbcError* error)

    CAdbcStatusCode AdbcConnectionCommit(
        CAdbcConnection* connection,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionRollback(
        CAdbcConnection* connection,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionDeserializePartitionDesc(
        CAdbcConnection* connection,
        const uint8_t* serialized_partition,
        size_t serialized_length,
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionGetInfo(
        CAdbcConnection* connection,
        uint32_t* info_codes,
        size_t info_codes_length,
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionGetObjects(
        CAdbcConnection* connection,
        int depth,
        const char* catalog,
        const char* db_schema,
        const char* table_name,
        const char** table_type,
        const char* column_name,
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionGetTableSchema(
        CAdbcConnection* connection,
        const char* catalog,
        const char* db_schema,
        const char* table_name,
        CArrowSchema* schema,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionGetTableTypes(
        CAdbcConnection* connection,
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionInit(
        CAdbcConnection* connection,
        CAdbcDatabase* database,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionNew(
        CAdbcConnection* connection,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionSetOption(
        CAdbcConnection* connection,
        const char* key,
        const char* value,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionRelease(
        CAdbcConnection* connection,
        CAdbcError* error)

    CAdbcStatusCode AdbcStatementBind(
        CAdbcStatement* statement,
        CArrowArray*,
        CArrowSchema*,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementBindStream(
        CAdbcStatement* statement,
        CArrowArrayStream*,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementExecute(
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementGetPartitionDesc(
        CAdbcStatement* statement,
        uint8_t* partition_desc,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementGetPartitionDescSize(
        CAdbcStatement* statement,
        size_t* length,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementGetStream(
        CAdbcStatement* statement,
        CArrowArrayStream* c_stream,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementNew(
        CAdbcConnection* connection,
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementPrepare(
        CAdbcStatement* statement,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementSetOption(
        CAdbcStatement* statement,
        const char* key,
        const char* value,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementSetSqlQuery(
        CAdbcStatement* statement,
        const char* query,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementSetSubstraitPlan(
        CAdbcStatement* statement,
        const uint8_t* plan,
        size_t length,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementRelease(
        CAdbcStatement* statement,
        CAdbcError* error)


cdef extern from "adbc_driver_manager.h":
    const char* CAdbcStatusCodeMessage"AdbcStatusCodeMessage"(CAdbcStatusCode code)


class AdbcStatusCode(enum.IntEnum):
    OK = ADBC_STATUS_OK
    UNKNOWN = ADBC_STATUS_UNKNOWN
    NOT_IMPLEMENTED = ADBC_STATUS_NOT_IMPLEMENTED
    NOT_FOUND = ADBC_STATUS_NOT_FOUND
    ALREADY_EXISTS = ADBC_STATUS_ALREADY_EXISTS
    INVALID_ARGUMENT = ADBC_STATUS_INVALID_ARGUMENT
    INVALID_STATE = ADBC_STATUS_INVALID_STATE
    INVALID_DATA = ADBC_STATUS_INVALID_DATA
    INTEGRITY = ADBC_STATUS_INTEGRITY
    INTERNAL = ADBC_STATUS_INTERNAL
    IO = ADBC_STATUS_IO
    CANCELLED = ADBC_STATUS_CANCELLED
    TIMEOUT = ADBC_STATUS_TIMEOUT
    UNAUTHENTICATED = ADBC_STATUS_UNAUTHENTICATED
    UNAUTHORIZED = ADBC_STATUS_UNAUTHORIZED


class AdbcInfoCode(enum.IntEnum):
    VENDOR_NAME = ADBC_INFO_VENDOR_NAME
    VENDOR_VERSION = ADBC_INFO_VENDOR_VERSION
    VENDOR_ARROW_VERSION = ADBC_INFO_VENDOR_ARROW_VERSION
    DRIVER_NAME = ADBC_INFO_DRIVER_NAME
    DRIVER_VERSION = ADBC_INFO_DRIVER_VERSION
    DRIVER_ARROW_VERSION = ADBC_INFO_DRIVER_ARROW_VERSION


class Error(Exception):
    """PEP-249 compliant base exception class.

    Attributes
    ----------
    status_code : CAdbcStatusCode
        The original ADBC status code.
    vendor_code : int, optional
        A vendor-specific status code if present.
    sqlstate : str, optional
        The SQLSTATE code if present.
    """

    def __init__(self, message, *, status_code, vendor_code=None, sqlstate=None):
        super().__init__(message)
        self.status_code = AdbcStatusCode(status_code)
        self.vendor_code = None
        self.sqlstate = None


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


INGEST_OPTION_TARGET_TABLE = ADBC_INGEST_OPTION_TARGET_TABLE.decode("utf-8")


cdef void check_error(CAdbcStatusCode status, CAdbcError* error) except *:
    if status == ADBC_STATUS_OK:
        return

    message = CAdbcStatusCodeMessage(status).decode("utf-8")
    vendor_code = None
    sqlstate = None

    if error != NULL:
        if error.message != NULL:
            message += ": "
            message += error.message.decode("utf-8")
        if error.vendor_code:
            vendor_code = error.vendor_code
        if error.sqlstate[0] != 0:
            sqlstate = error.sqlstate.decode("ascii")
        if error.release:
            error.release(error)

    klass = Error
    if status in (ADBC_STATUS_INVALID_DATA,):
        klass = DataError
    elif status in (ADBC_STATUS_IO, ADBC_STATUS_CANCELLED, ADBC_STATUS_TIMEOUT):
        klass = OperationalError
    elif status in (ADBC_STATUS_INTEGRITY,):
        klass = IntegrityError
    elif status in (ADBC_STATUS_INTERNAL,):
        klass = InternalError
    elif status in (ADBC_STATUS_ALREADY_EXISTS,
                    ADBC_STATUS_INVALID_ARGUMENT,
                    ADBC_STATUS_INVALID_STATE,
                    ADBC_STATUS_UNAUTHENTICATED,
                    ADBC_STATUS_UNAUTHORIZED):
        klass = ProgrammingError
    elif status == ADBC_STATUS_NOT_IMPLEMENTED:
        klass = NotSupportedError
    raise klass(message, status_code=status)


cdef CAdbcError empty_error():
    cdef CAdbcError error
    memset(&error, 0, cython.sizeof(error))
    return error


cdef bytes _to_bytes(obj, str name):
    if isinstance(obj, bytes):
        return obj
    elif isinstance(obj, str):
        return obj.encode("utf-8")
    raise ValueError(f"{name} must be str or bytes")


cdef class _AdbcHandle:
    """
    Base class for ADBC handles, which are context managers.
    """
    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


cdef class ArrowSchemaHandle:
    """
    A wrapper for an allocated ArrowSchema.
    """
    cdef:
        CArrowSchema schema

    @property
    def address(self) -> int:
        """The address of the ArrowSchema."""
        return <uintptr_t> &self.schema


cdef class ArrowArrayHandle:
    """
    A wrapper for an allocated ArrowArray.
    """
    cdef:
        CArrowArray array

    @property
    def address(self) -> int:
        """The address of the ArrowArray."""
        return <uintptr_t> &self.array


cdef class ArrowArrayStreamHandle:
    """
    A wrapper for an allocated ArrowArrayStream.
    """
    cdef:
        CArrowArrayStream stream

    @property
    def address(self) -> int:
        """The address of the ArrowArrayStream."""
        return <uintptr_t> &self.stream


class GetObjectsDepth(enum.IntEnum):
    ALL = ADBC_OBJECT_DEPTH_ALL
    CATALOGS = ADBC_OBJECT_DEPTH_CATALOGS
    DB_SCHEMAS = ADBC_OBJECT_DEPTH_DB_SCHEMAS
    TABLES = ADBC_OBJECT_DEPTH_TABLES
    COLUMNS = ADBC_OBJECT_DEPTH_COLUMNS


cdef class AdbcDatabase(_AdbcHandle):
    """
    An instance of a database.

    Parameters
    ----------
    kwargs : dict
        String key-value options to pass to the underlying database.
        Must include at least "driver" and "entrypoint" to identify
        the underlying database driver to load.
    """
    cdef:
        CAdbcDatabase database

    def __init__(self, **kwargs) -> None:
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef const char* c_key
        cdef const char* c_value
        memset(&self.database, 0, cython.sizeof(CAdbcDatabase))

        status = AdbcDatabaseNew(&self.database, &c_error)
        check_error(status, &c_error)

        for key, value in kwargs.items():
            key = key.encode("utf-8")
            value = value.encode("utf-8")
            c_key = key
            c_value = value
            status = AdbcDatabaseSetOption(&self.database, c_key, c_value, &c_error)
            check_error(status, &c_error)

        status = AdbcDatabaseInit(&self.database, &c_error)
        check_error(status, &c_error)

    def close(self) -> None:
        """Release the handle to the database."""
        if self.database.private_data == NULL:
            return

        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status = AdbcDatabaseRelease(&self.database, &c_error)
        check_error(status, &c_error)


cdef class AdbcConnection(_AdbcHandle):
    """
    An active database connection.

    Connections are not thread-safe and clients should take care to
    serialize accesses to a connection.

    Parameters
    ----------
    database : AdbcDatabase
        The database to connect to.
    kwargs : dict
        String key-value options to pass to the underlying database.
    """
    cdef:
        AdbcDatabase database
        CAdbcConnection connection

    def __init__(self, AdbcDatabase database, **kwargs) -> None:
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef const char* c_key
        cdef const char* c_value

        self.database = database
        memset(&self.connection, 0, cython.sizeof(CAdbcConnection))

        status = AdbcConnectionNew(&self.connection, &c_error)
        check_error(status, &c_error)

        for key, value in kwargs.items():
            key = key.encode("utf-8")
            value = value.encode("utf-8")
            c_key = key
            c_value = value
            status = AdbcConnectionSetOption(&self.connection, c_key, c_value, &c_error)
            check_error(status, &c_error)

        status = AdbcConnectionInit(&self.connection, &database.database, &c_error)
        check_error(status, &c_error)

    def commit(self) -> None:
        """Commit the current transaction."""
        cdef CAdbcError c_error = empty_error()
        check_error(AdbcConnectionCommit(&self.connection, &c_error), &c_error)

    def get_info(self, info_codes=None):
        """
        Get metadata about the database/driver.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef AdbcStatement statement = AdbcStatement(self)
        cdef c_vector[uint32_t] c_info_codes

        if info_codes:
            for info_code in info_codes:
                if isinstance(info_code, int):
                    c_info_codes.push_back(info_code)
                else:
                    c_info_codes.push_back(info_code.value)

            status = AdbcConnectionGetInfo(
                &self.connection,
                c_info_codes.data(),
                c_info_codes.size(),
                &statement.statement,
                &c_error)
        else:
            status = AdbcConnectionGetInfo(
                &self.connection,
                NULL,
                0,
                &statement.statement,
                &c_error)

        check_error(status, &c_error)
        return statement

    def get_objects(self, depth, catalog=None, db_schema=None, table_name=None,
                    table_types=None, column_name=None) -> AdbcStatement:
        """
        Get a hierarchical view of database objects.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef AdbcStatement statement = AdbcStatement(self)

        cdef char* c_catalog = NULL
        if catalog is not None:
            catalog = _to_bytes(catalog, "catalog")
            c_catalog = catalog

        cdef char* c_db_schema = NULL
        if db_schema is not None:
            db_schema = _to_bytes(db_schema, "db_schema")
            c_db_schema = db_schema

        cdef char* c_table_name = NULL
        if table_name is not None:
            table_name = _to_bytes(table_name, "table_name")
            c_table_name = table_name

        cdef char* c_column_name = NULL
        if column_name is not None:
            column_name = _to_bytes(column_name, "column_name")
            c_column_name = column_name

        status = AdbcConnectionGetObjects(
            &self.connection,
            GetObjectsDepth(depth).value,
            c_catalog,
            c_db_schema,
            c_table_name,
            NULL,  # TODO: support table_types
            c_column_name,
            &statement.statement,
            &c_error)
        check_error(status, &c_error)

        return statement

    def get_table_schema(self, catalog, db_schema, table_name) -> ArrowSchemaHandle:
        """
        Get the Arrow schema of a table.

        Returns
        -------
        ArrowSchemaHandle
            A C Data Interface ArrowSchema struct containing the schema.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef ArrowSchemaHandle handle = ArrowSchemaHandle()

        cdef char* c_catalog = NULL
        if catalog is not None:
            catalog = _to_bytes(catalog, "catalog")
            c_catalog = catalog

        cdef char* c_db_schema = NULL
        if db_schema is not None:
            db_schema = _to_bytes(db_schema, "db_schema")
            c_db_schema = db_schema

        status = AdbcConnectionGetTableSchema(
            &self.connection,
            c_catalog,
            c_db_schema,
            _to_bytes(table_name, "table_name"),
            &handle.schema,
            &c_error)
        check_error(status, &c_error)

        return handle

    def get_table_types(self) -> AdbcStatement:
        """
        Get the list of supported table types.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef AdbcStatement statement = AdbcStatement(self)

        status = AdbcConnectionGetTableTypes(
            &self.connection, &statement.statement, &c_error)
        check_error(status, &c_error)

        return statement

    def rollback(self) -> None:
        """Rollback the current transaction."""
        cdef CAdbcError c_error = empty_error()
        check_error(AdbcConnectionRollback(&self.connection, &c_error), &c_error)

    def set_autocommit(self, bint enabled) -> None:
        """Toggle whether autocommit is enabled."""
        cdef CAdbcError c_error = empty_error()
        if enabled:
            value = ADBC_OPTION_VALUE_ENABLED
        else:
            value = ADBC_OPTION_VALUE_DISABLED
        status = AdbcConnectionSetOption(
            &self.connection,
            ADBC_CONNECTION_OPTION_AUTOCOMMIT,
            value,
            &c_error)
        check_error(status, &c_error)

    def close(self) -> None:
        """Release the handle to the connection."""
        if self.connection.private_data == NULL:
            return

        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status = AdbcConnectionRelease(&self.connection, &c_error)
        check_error(status, &c_error)


cdef class AdbcStatement(_AdbcHandle):
    """
    A database statement.

    Statements are not thread-safe and clients should take care to
    serialize accesses to a connection.

    Parameters
    ----------
    connection : AdbcConnection
        The connection to create the statement for.
    """
    cdef:
        CAdbcStatement statement

    def __init__(self, AdbcConnection connection) -> None:
        cdef CAdbcError c_error = empty_error()
        memset(&self.statement, 0, cython.sizeof(CAdbcStatement))

        status = AdbcStatementNew(&connection.connection, &self.statement, &c_error)
        check_error(status, &c_error)

    def bind(self, data, schema) -> None:
        """
        Bind an ArrowArray to this statement.

        Parameters
        ----------
        data : int or ArrowArrayHandle
        schema : int or ArrowSchemaHandle
        """
        cdef CAdbcError c_error = empty_error()
        cdef CArrowArray* c_array
        cdef CArrowSchema* c_schema

        if isinstance(data, ArrowArrayHandle):
            c_array = &(<ArrowArrayHandle> data).array
        elif isinstance(data, int):
            c_array = <CArrowArray*> data
        else:
            raise TypeError(f"data must be int or ArrowArrayHandle, not {type(data)}")

        if isinstance(schema, ArrowSchemaHandle):
            c_schema = &(<ArrowSchemaHandle> schema).schema
        elif isinstance(schema, int):
            c_schema = <CArrowSchema*> schema
        else:
            raise TypeError(f"schema must be int or ArrowSchemaHandle, "
                            f"not {type(schema)}")

        status = AdbcStatementBind(&self.statement, c_array, c_schema, &c_error)
        check_error(status, &c_error)

    def bind_stream(self, stream) -> None:
        """
        Bind an ArrowArrayStream to this statement.

        Parameters
        ----------
        stream : int or ArrowArrayStreamHandle
        """
        cdef CAdbcError c_error = empty_error()
        cdef CArrowArrayStream* c_stream

        if isinstance(stream, ArrowArrayStreamHandle):
            c_stream = &(<ArrowArrayStreamHandle> stream).stream
        elif isinstance(stream, int):
            c_stream = <CArrowArrayStream*> stream
        else:
            raise TypeError(f"data must be int or ArrowArrayStreamHandle, "
                            f"not {type(stream)}")

        status = AdbcStatementBindStream(&self.statement, c_stream, &c_error)
        check_error(status, &c_error)

    def close(self) -> None:
        if self.statement.private_data == NULL:
            return

        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status = AdbcStatementRelease(&self.statement, &c_error)
        check_error(status, &c_error)

    def execute(self) -> None:
        """Execute the query."""
        cdef CAdbcError c_error = empty_error()
        with nogil:
            status = AdbcStatementExecute(&self.statement, &c_error)
        check_error(status, &c_error)

    def get_partitions(self) -> List[bytes]:
        """Get the partitions of a distributed result set."""
        cdef CAdbcError c_error = empty_error()
        cdef size_t length = 0
        cdef bytes buf
        cdef uint8_t* c_buf

        result = []
        while True:
            with nogil:
                status = AdbcStatementGetPartitionDescSize(
                        &self.statement,
                        &length,
                        &c_error,
                    )
            check_error(status, &c_error)
            if length == 0:
                break

            buf = bytes(length)
            c_buf = <uint8_t*> buf
            with nogil:
                status = AdbcStatementGetPartitionDesc(
                    &self.statement,
                    c_buf,
                    &c_error,
                )
            check_error(status, &c_error)
            result.append(buf)

        return result

    def get_stream(self) -> ArrowArrayStreamHandle:
        """Get a reader for the result set."""
        cdef CAdbcError c_error = empty_error()
        cdef ArrowArrayStreamHandle stream = ArrowArrayStreamHandle()
        status = AdbcStatementGetStream(&self.statement, &stream.stream, &c_error)
        check_error(status, &c_error)
        return stream

    def prepare(self) -> None:
        """Turn this statement into a prepared statement."""
        cdef CAdbcError c_error = empty_error()
        status = AdbcStatementPrepare(&self.statement, &c_error)
        check_error(status, &c_error)

    def set_options(self, **kwargs) -> None:
        """Set arbitrary key-value options."""
        cdef CAdbcError c_error = empty_error()
        for key, value in kwargs.items():
            key = key.encode("utf-8")
            value = value.encode("utf-8")
            c_key = key
            c_value = value
            status = AdbcStatementSetOption(
                &self.statement, c_key, c_value, &c_error)
            check_error(status, &c_error)

    def set_sql_query(self, query: str) -> None:
        """Set a SQL query to be executed."""
        cdef CAdbcError c_error = empty_error()
        status = AdbcStatementSetSqlQuery(
            &self.statement, query.encode("utf-8"), &c_error)
        check_error(status, &c_error)

    def set_substrait_plan(self, plan: bytes) -> None:
        """Set a Substrait plan to be executed."""
        cdef CAdbcError c_error = empty_error()
        status = AdbcStatementSetSubstraitPlan(
            &self.statement, plan, len(plan), &c_error)
        check_error(status, &c_error)
