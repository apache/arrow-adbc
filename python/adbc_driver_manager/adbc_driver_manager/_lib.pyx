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
import threading
import typing
from typing import List, Tuple

import cython
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uintptr_t
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
    cdef const char* ADBC_INGEST_OPTION_MODE
    cdef const char* ADBC_INGEST_OPTION_MODE_APPEND
    cdef const char* ADBC_INGEST_OPTION_MODE_CREATE

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
    ctypedef void (*CAdbcPartitionsRelease)(CAdbcPartitions*)

    cdef struct CAdbcError"AdbcError":
        char* message
        int32_t vendor_code
        char[5] sqlstate
        CAdbcErrorRelease release

    cdef struct CAdbcDriver"AdbcDriver":
        pass

    cdef struct CAdbcDatabase"AdbcDatabase":
        void* private_data

    cdef struct CAdbcConnection"AdbcConnection":
        void* private_data

    cdef struct CAdbcStatement"AdbcStatement":
        void* private_data

    cdef struct CAdbcPartitions"AdbcPartitions":
        size_t num_partitions
        const uint8_t** partitions
        const size_t* partition_lengths
        void* private_data
        CAdbcPartitionsRelease release

    CAdbcStatusCode AdbcDatabaseNew(CAdbcDatabase* database, CAdbcError* error)
    CAdbcStatusCode AdbcDatabaseSetOption(
        CAdbcDatabase* database,
        const char* key,
        const char* value,
        CAdbcError* error)
    CAdbcStatusCode AdbcDatabaseInit(CAdbcDatabase* database, CAdbcError* error)
    CAdbcStatusCode AdbcDatabaseRelease(CAdbcDatabase* database, CAdbcError* error)

    ctypedef void (*CAdbcDriverInitFunc "AdbcDriverInitFunc")(int, void*, CAdbcError*)
    CAdbcStatusCode AdbcDriverManagerDatabaseSetInitFunc(
        CAdbcDatabase* database,
        CAdbcDriverInitFunc init_func,
        CAdbcError* error)

    CAdbcStatusCode AdbcConnectionCommit(
        CAdbcConnection* connection,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionRollback(
        CAdbcConnection* connection,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionReadPartition(
        CAdbcConnection* connection,
        const uint8_t* serialized_partition,
        size_t serialized_length,
        CArrowArrayStream* out,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionGetInfo(
        CAdbcConnection* connection,
        uint32_t* info_codes,
        size_t info_codes_length,
        CArrowArrayStream* stream,
        CAdbcError* error)
    CAdbcStatusCode AdbcConnectionGetObjects(
        CAdbcConnection* connection,
        int depth,
        const char* catalog,
        const char* db_schema,
        const char* table_name,
        const char** table_type,
        const char* column_name,
        CArrowArrayStream* stream,
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
        CArrowArrayStream* stream,
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
    CAdbcStatusCode AdbcStatementExecutePartitions(
        CAdbcStatement* statement,
        CArrowSchema* schema, CAdbcPartitions* partitions,
        int64_t* rows_affected,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementExecuteQuery(
        CAdbcStatement* statement,
        CArrowArrayStream* out, int64_t* rows_affected,
        CAdbcError* error)
    CAdbcStatusCode AdbcStatementGetParameterSchema(
        CAdbcStatement* statement,
        CArrowSchema* schema,
        CAdbcError* error);
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
    """
    A status code indicating the type of error.
    """

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


class Warning(UserWarning):
    """
    PEP 249-compliant base warning class.
    """


class Error(Exception):
    """
    PEP 249-compliant base exception class.

    Attributes
    ----------
    status_code : AdbcStatusCode
        The original ADBC status code.
    vendor_code : int, optional
        A vendor-specific status code if present.
    sqlstate : str, optional
        The SQLSTATE code if present.
    """

    def __init__(self, message, *, status_code, vendor_code=None, sqlstate=None):
        super().__init__(message)
        self.status_code = AdbcStatusCode(status_code)
        self.vendor_code = vendor_code
        self.sqlstate = sqlstate


class InterfaceError(Error):
    """Errors related to the database interface."""


class DatabaseError(Error):
    """Errors related to the database."""


class DataError(DatabaseError):
    """Errors related to processed data."""


class OperationalError(DatabaseError):
    """Errors related to database operation, not under user control."""


class IntegrityError(DatabaseError):
    """Errors related to relational integrity."""


class InternalError(DatabaseError):
    """Errors related to database-internal errors."""


class ProgrammingError(DatabaseError):
    """Errors related to user errors."""


class NotSupportedError(DatabaseError):
    """An operation or some functionality is not supported."""

    def __init__(self, message, *, vendor_code=None, sqlstate=None):
        super().__init__(
            message,
            status_code=AdbcStatusCode.NOT_IMPLEMENTED,
            vendor_code=vendor_code,
            sqlstate=sqlstate,
        )


INGEST_OPTION_MODE = ADBC_INGEST_OPTION_MODE.decode("utf-8")
INGEST_OPTION_MODE_APPEND = ADBC_INGEST_OPTION_MODE_APPEND.decode("utf-8")
INGEST_OPTION_MODE_CREATE = ADBC_INGEST_OPTION_MODE_CREATE.decode("utf-8")
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
            message += f". Vendor code: {vendor_code}"
        if error.sqlstate[0] != 0:
            sqlstate = bytes(error.sqlstate[i] for i in range(5))
            sqlstate = sqlstate.decode("ascii")
            message += f". SQLSTATE: {sqlstate}"
        if error.release:
            error.release(error)

    klass = Error
    if status in (ADBC_STATUS_INVALID_DATA,):
        klass = DataError
    elif status in (
        ADBC_STATUS_IO,
        ADBC_STATUS_CANCELLED,
        ADBC_STATUS_TIMEOUT,
        ADBC_STATUS_UNKNOWN,
    ):
        klass = OperationalError
    elif status in (ADBC_STATUS_INTEGRITY,):
        klass = IntegrityError
    elif status in (ADBC_STATUS_INTERNAL,):
        klass = InternalError
    elif status in (ADBC_STATUS_ALREADY_EXISTS,
                    ADBC_STATUS_INVALID_ARGUMENT,
                    ADBC_STATUS_INVALID_STATE,
                    ADBC_STATUS_NOT_FOUND,
                    ADBC_STATUS_UNAUTHENTICATED,
                    ADBC_STATUS_UNAUTHORIZED):
        klass = ProgrammingError
    elif status == ADBC_STATUS_NOT_IMPLEMENTED:
        raise NotSupportedError(message, vendor_code=vendor_code, sqlstate=sqlstate)
    raise klass(message, status_code=status, vendor_code=vendor_code, sqlstate=sqlstate)


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


def _test_error(status_code, message, vendor_code, sqlstate) -> Error:
    cdef CAdbcError error
    error.release = NULL

    message = _to_bytes(message, "message")
    error.message = message

    if vendor_code:
        error.vendor_code = vendor_code
    else:
        error.vendor_code = 0

    if sqlstate:
        sqlstate = sqlstate.encode("ascii")
    else:
        sqlstate = b"\0\0\0\0\0"
    for i in range(5):
        error.sqlstate[i] = sqlstate[i]

    return check_error(AdbcStatusCode(status_code), &error)


cdef class _AdbcHandle:
    """
    Base class for ADBC handles, which are context managers.
    """

    cdef:
        size_t _open_children
        object _lock
        str _child_type

    def __init__(self, str child_type):
        self._lock = threading.Lock()
        self._child_type = child_type

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    cdef _open_child(self):
        with self._lock:
            self._open_children += 1

    cdef _close_child(self):
        with self._lock:
            if self._open_children == 0:
                raise RuntimeError(
                    f"Underflow in closing this {self._child_type}")
            self._open_children -= 1

    cdef _check_open_children(self):
        with self._lock:
            if self._open_children != 0:
                raise RuntimeError(
                    f"Cannot close {self.__class__.__name__} "
                    f"with open {self._child_type}")


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
        Must include at least "driver" to identify the underlying
        database driver to load.
    """
    cdef:
        CAdbcDatabase database

    def __init__(self, **kwargs) -> None:
        super().__init__("AdbcConnection")
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef const char* c_key
        cdef const char* c_value
        memset(&self.database, 0, cython.sizeof(CAdbcDatabase))

        with nogil:
            status = AdbcDatabaseNew(&self.database, &c_error)
        check_error(status, &c_error)

        for key, value in kwargs.items():
            if key == "init_func":
                status = AdbcDriverManagerDatabaseSetInitFunc(
                    &self.database, <CAdbcDriverInitFunc> (<uintptr_t> value), &c_error)
            elif key is None:
                raise ValueError("key cannot be None")
            elif value is None:
                raise ValueError(f"value for key '{key}' cannot be None")
            else:
                key = key.encode("utf-8")
                value = value.encode("utf-8")
                c_key = key
                c_value = value
                status = AdbcDatabaseSetOption(
                    &self.database, c_key, c_value, &c_error)
            check_error(status, &c_error)

        with nogil:
            status = AdbcDatabaseInit(&self.database, &c_error)
        check_error(status, &c_error)

    def close(self) -> None:
        """Release the handle to the database."""
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        self._check_open_children()
        with self._lock:
            if self.database.private_data == NULL:
                return

            with nogil:
                status = AdbcDatabaseRelease(&self.database, &c_error)
            check_error(status, &c_error)

    def set_options(self, **kwargs) -> None:
        """Set arbitrary key-value options.

        Note, not all drivers support setting options after creation.

        See Also
        --------
        adbc_driver_manager.DatabaseOptions : Standard option names.

        """
        cdef CAdbcError c_error = empty_error()
        cdef char* c_key = NULL
        cdef char* c_value = NULL
        for key, value in kwargs.items():
            key = key.encode("utf-8")
            c_key = key

            if value is None:
                c_value = NULL
            else:
                value = value.encode("utf-8")
                c_value = value

            status = AdbcDatabaseSetOption(
                &self.database, c_key, c_value, &c_error)
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
        super().__init__("AdbcStatement")
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef const char* c_key
        cdef const char* c_value

        self.database = database
        memset(&self.connection, 0, cython.sizeof(CAdbcConnection))

        with nogil:
            status = AdbcConnectionNew(&self.connection, &c_error)
        check_error(status, &c_error)

        for key, value in kwargs.items():
            key = key.encode("utf-8")
            value = value.encode("utf-8")
            c_key = key
            c_value = value
            status = AdbcConnectionSetOption(&self.connection, c_key, c_value, &c_error)
            if status != ADBC_STATUS_OK:
                AdbcConnectionRelease(&self.connection, NULL)
            check_error(status, &c_error)

        with nogil:
            status = AdbcConnectionInit(&self.connection, &database.database, &c_error)
            if status != ADBC_STATUS_OK:
                AdbcConnectionRelease(&self.connection, NULL)
        check_error(status, &c_error)

        database._open_child()

    def commit(self) -> None:
        """Commit the current transaction."""
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        with nogil:
            status = AdbcConnectionCommit(&self.connection, &c_error)
        check_error(status, &c_error)

    def get_info(self, info_codes=None) -> ArrowArrayStreamHandle:
        """
        Get metadata about the database/driver.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef ArrowArrayStreamHandle stream = ArrowArrayStreamHandle()
        cdef c_vector[uint32_t] c_info_codes

        if info_codes:
            for info_code in info_codes:
                if isinstance(info_code, int):
                    c_info_codes.push_back(info_code)
                else:
                    c_info_codes.push_back(info_code.value)

            with nogil:
                status = AdbcConnectionGetInfo(
                    &self.connection,
                    c_info_codes.data(),
                    c_info_codes.size(),
                    &stream.stream,
                    &c_error)
        else:
            with nogil:
                status = AdbcConnectionGetInfo(
                    &self.connection,
                    NULL,
                    0,
                    &stream.stream,
                    &c_error)

        check_error(status, &c_error)
        return stream

    def get_objects(self, depth, catalog=None, db_schema=None, table_name=None,
                    table_types=None, column_name=None) -> ArrowArrayStreamHandle:
        """
        Get a hierarchical view of database objects.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef ArrowArrayStreamHandle stream = ArrowArrayStreamHandle()
        cdef int c_depth = GetObjectsDepth(depth).value

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

        with nogil:
            status = AdbcConnectionGetObjects(
                &self.connection,
                c_depth,
                c_catalog,
                c_db_schema,
                c_table_name,
                NULL,  # TODO: support table_types
                c_column_name,
                &stream.stream,
                &c_error)
        check_error(status, &c_error)

        return stream

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
        table_name = _to_bytes(table_name, "table_name")
        cdef char* c_table_name = table_name

        cdef char* c_catalog = NULL
        if catalog is not None:
            catalog = _to_bytes(catalog, "catalog")
            c_catalog = catalog

        cdef char* c_db_schema = NULL
        if db_schema is not None:
            db_schema = _to_bytes(db_schema, "db_schema")
            c_db_schema = db_schema

        with nogil:
            status = AdbcConnectionGetTableSchema(
                &self.connection,
                c_catalog,
                c_db_schema,
                c_table_name,
                &handle.schema,
                &c_error)
        check_error(status, &c_error)
        return handle

    def get_table_types(self) -> ArrowArrayStreamHandle:
        """
        Get the list of supported table types.
        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef ArrowArrayStreamHandle stream = ArrowArrayStreamHandle()

        with nogil:
            status = AdbcConnectionGetTableTypes(
                &self.connection, &stream.stream, &c_error)
        check_error(status, &c_error)
        return stream

    def read_partition(self, bytes partition not None) -> ArrowArrayStreamHandle:
        """Fetch a single partition from execute_partitions."""
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef ArrowArrayStreamHandle stream = ArrowArrayStreamHandle()
        cdef const uint8_t* data = <const uint8_t*> partition
        cdef size_t length = len(partition)

        with nogil:
            status = AdbcConnectionReadPartition(
                &self.connection,
                data,
                length,
                &stream.stream,
                &c_error)
        check_error(status, &c_error)
        return stream

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

        with nogil:
            status = AdbcConnectionSetOption(
                &self.connection,
                ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                value,
                &c_error)
        check_error(status, &c_error)

    def set_options(self, **kwargs) -> None:
        """Set arbitrary key-value options.

        Note, not all drivers support setting options after creation.

        See Also
        --------
        adbc_driver_manager.ConnectionOptions : Standard option names.
        """
        cdef CAdbcError c_error = empty_error()
        cdef char* c_key = NULL
        cdef char* c_value = NULL
        for key, value in kwargs.items():
            key = key.encode("utf-8")
            c_key = key

            if value is None:
                c_value = NULL
            else:
                value = value.encode("utf-8")
                c_value = value

            status = AdbcConnectionSetOption(
                &self.connection, c_key, c_value, &c_error)
            check_error(status, &c_error)

    def close(self) -> None:
        """Release the handle to the connection."""
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        self._check_open_children()
        with self._lock:
            if self.connection.private_data == NULL:
                return

            with nogil:
                status = AdbcConnectionRelease(&self.connection, &c_error)
            check_error(status, &c_error)
            self.database._close_child()


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
        AdbcConnection connection
        CAdbcStatement statement

    def __init__(self, AdbcConnection connection) -> None:
        super().__init__("(no child type)")
        cdef CAdbcError c_error = empty_error()
        self.connection = connection
        memset(&self.statement, 0, cython.sizeof(CAdbcStatement))

        with nogil:
            status = AdbcStatementNew(
                &connection.connection,
                &self.statement,
                &c_error)
        check_error(status, &c_error)

        connection._open_child()

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

        with nogil:
            status = AdbcStatementBind(
                &self.statement,
                c_array,
                c_schema,
                &c_error)
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

        with nogil:
            status = AdbcStatementBindStream(
                &self.statement,
                c_stream,
                &c_error)
        check_error(status, &c_error)

    def close(self) -> None:
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        self.connection._close_child()
        with self._lock:
            if self.statement.private_data == NULL:
                return

            with nogil:
                status = AdbcStatementRelease(&self.statement, &c_error)
            check_error(status, &c_error)

    def execute_query(self) -> Tuple[ArrowArrayStreamHandle, int]:
        """
        Execute the query and get the result set.

        Returns
        -------
        ArrowArrayStreamHandle
            The result set.
        int
            The number of rows if known, else -1.
        """
        cdef CAdbcError c_error = empty_error()
        cdef ArrowArrayStreamHandle stream = ArrowArrayStreamHandle()
        cdef int64_t rows_affected = 0
        with nogil:
            status = AdbcStatementExecuteQuery(
                &self.statement,
                &stream.stream,
                &rows_affected,
                &c_error)
        check_error(status, &c_error)
        return (stream, rows_affected)

    def execute_partitions(self) -> Tuple[List[bytes], ArrowSchemaHandle, int]:
        """
        Execute the query and get the partitions of the result set.

        Not all drivers will support this.

        Returns
        -------
        list of byte
            The partitions of the distributed result set.
        ArrowSchemaHandle
            The schema of the result set.
        int
            The number of rows if known, else -1.
        """
        cdef CAdbcError c_error = empty_error()
        cdef ArrowSchemaHandle schema = ArrowSchemaHandle()
        cdef CAdbcPartitions c_partitions = CAdbcPartitions(
            0, NULL, NULL, NULL, NULL)
        cdef int64_t rows_affected = 0

        with nogil:
            status = AdbcStatementExecutePartitions(
                &self.statement,
                &schema.schema,
                &c_partitions,
                &rows_affected,
                &c_error)
        check_error(status, &c_error)

        partitions = []
        for i in range(c_partitions.num_partitions):
            length = c_partitions.partition_lengths[i]
            data = <const char*> c_partitions.partitions[i]
            partitions.append(PyBytes_FromStringAndSize(data, length))
        c_partitions.release(&c_partitions)

        return (partitions, schema, rows_affected)

    def execute_update(self) -> int:
        """
        Execute the query without a result set.

        Returns
        -------
        int
            The number of affected rows if known, else -1.
        """
        cdef CAdbcError c_error = empty_error()
        cdef int64_t rows_affected = 0
        with nogil:
            status = AdbcStatementExecuteQuery(
                &self.statement,
                NULL,
                &rows_affected,
                &c_error)
        check_error(status, &c_error)
        return rows_affected

    def get_parameter_schema(self) -> ArrowSchemaHandle:
        """Get the Arrow schema for bound parameters.

        This retrieves an Arrow schema describing the number, names,
        and types of the parameters in a parameterized statement.  The
        fields of the schema should be in order of the ordinal
        position of the parameters; named parameters should appear
        only once.

        If the parameter does not have a name, or the name cannot be
        determined, the name of the corresponding field in the schema
        will be an empty string.  If the type cannot be determined,
        the type of the corresponding field will be NA (NullType).

        This should be called after :meth:`prepare`.

        Raises
        ------
        NotSupportedError
            If the schema could not be determined.

        """
        cdef CAdbcError c_error = empty_error()
        cdef CAdbcStatusCode status
        cdef ArrowSchemaHandle handle = ArrowSchemaHandle()

        with nogil:
            status = AdbcStatementGetParameterSchema(
                &self.statement, &handle.schema, &c_error)
        check_error(status, &c_error)
        return handle

    def prepare(self) -> None:
        """Turn this statement into a prepared statement."""
        cdef CAdbcError c_error = empty_error()
        with nogil:
            status = AdbcStatementPrepare(&self.statement, &c_error)
        check_error(status, &c_error)

    def set_options(self, **kwargs) -> None:
        """Set arbitrary key-value options.

        See Also
        --------
        adbc_driver_manager.StatementOptions : Standard option names.
        """
        cdef CAdbcError c_error = empty_error()
        cdef char* c_key = NULL
        cdef char* c_value = NULL
        for key, value in kwargs.items():
            key = key.encode("utf-8")
            c_key = key

            if value is None:
                c_value = NULL
            else:
                value = value.encode("utf-8")
                c_value = value

            status = AdbcStatementSetOption(
                &self.statement, c_key, c_value, &c_error)
            check_error(status, &c_error)

    def set_sql_query(self, str query not None) -> None:
        """Set a SQL query to be executed."""
        cdef CAdbcError c_error = empty_error()
        cdef bytes query_data = query.encode("utf-8")
        cdef const char* c_query = query_data
        with nogil:
            status = AdbcStatementSetSqlQuery(
                &self.statement, c_query, &c_error)
        check_error(status, &c_error)

    def set_substrait_plan(self, bytes plan not None) -> None:
        """Set a Substrait plan to be executed."""
        cdef CAdbcError c_error = empty_error()
        cdef const uint8_t* c_plan = <const uint8_t*> plan
        cdef size_t length = len(plan)
        with nogil:
            status = AdbcStatementSetSubstraitPlan(
                &self.statement, c_plan, length, &c_error)
        check_error(status, &c_error)
