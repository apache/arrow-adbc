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

import typing

import cython
import pyarrow
from libc.stdint cimport int32_t, uint8_t, uintptr_t
from libc.string cimport memset

if typing.TYPE_CHECKING:
    from typing import Self


cdef extern from "adbc.h":
    # C ABI
    cdef struct CArrowSchema"ArrowSchema":
        pass
    cdef struct CArrowArray"ArrowArray":
        pass
    cdef struct CArrowArrayStream"ArrowArrayStream":
        pass

    # ADBC
    ctypedef uint8_t AdbcStatusCode
    cdef AdbcStatusCode ADBC_STATUS_OK
    cdef AdbcStatusCode ADBC_STATUS_UNKNOWN
    cdef AdbcStatusCode ADBC_STATUS_NOT_IMPLEMENTED
    cdef AdbcStatusCode ADBC_STATUS_NOT_FOUND
    cdef AdbcStatusCode ADBC_STATUS_ALREADY_EXISTS
    cdef AdbcStatusCode ADBC_STATUS_INVALID_ARGUMENT
    cdef AdbcStatusCode ADBC_STATUS_INVALID_STATE
    cdef AdbcStatusCode ADBC_STATUS_INVALID_DATA
    cdef AdbcStatusCode ADBC_STATUS_INTEGRITY
    cdef AdbcStatusCode ADBC_STATUS_INTERNAL
    cdef AdbcStatusCode ADBC_STATUS_IO
    cdef AdbcStatusCode ADBC_STATUS_CANCELLED
    cdef AdbcStatusCode ADBC_STATUS_TIMEOUT
    cdef AdbcStatusCode ADBC_STATUS_UNAUTHENTICATED
    cdef AdbcStatusCode ADBC_STATUS_UNAUTHORIZED

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

    AdbcStatusCode AdbcDatabaseNew(CAdbcDatabase* database, CAdbcError* error)
    AdbcStatusCode AdbcDatabaseSetOption(CAdbcDatabase* database, const char* key, const char* value, CAdbcError* error)
    AdbcStatusCode AdbcDatabaseInit(CAdbcDatabase* database, CAdbcError* error)
    AdbcStatusCode AdbcDatabaseRelease(CAdbcDatabase* database, CAdbcError* error)

    AdbcStatusCode AdbcConnectionNew(CAdbcConnection* connection, CAdbcError* error)
    AdbcStatusCode AdbcConnectionSetOption(CAdbcConnection* connection, const char* key, const char* value, CAdbcError* error)
    AdbcStatusCode AdbcConnectionInit(CAdbcConnection* connection, CAdbcDatabase* database, CAdbcError* error)
    AdbcStatusCode AdbcConnectionRelease(CAdbcConnection* connection, CAdbcError* error)

    AdbcStatusCode AdbcStatementBind(CAdbcStatement* statement, CArrowArray*, CArrowSchema*, CAdbcError* error)
    AdbcStatusCode AdbcStatementBindStream(CAdbcStatement* statement, CArrowArrayStream*, CAdbcError* error)
    AdbcStatusCode AdbcStatementExecute(CAdbcStatement* statement, CAdbcError* error)
    AdbcStatusCode AdbcStatementGetStream(CAdbcStatement* statement, CArrowArrayStream* c_stream, CAdbcError* error)
    AdbcStatusCode AdbcStatementNew(CAdbcConnection* connection, CAdbcStatement* statement, CAdbcError* error)
    AdbcStatusCode AdbcStatementPrepare(CAdbcStatement* statement, CAdbcError* error)
    AdbcStatusCode AdbcStatementSetOption(CAdbcStatement* statement, const char* key, const char* value, CAdbcError* error)
    AdbcStatusCode AdbcStatementSetSqlQuery(CAdbcStatement* statement, const char* query, CAdbcError* error)
    AdbcStatusCode AdbcStatementRelease(CAdbcStatement* statement, CAdbcError* error)


cdef extern from "adbc_driver_manager.h":
    const char* AdbcStatusCodeMessage(AdbcStatusCode code)


INGEST_OPTION_TARGET_TABLE = "adbc.ingest.target_table"


class Error(Exception):
    """PEP-249 compliant base exception class.

    Attributes
    ----------
    status_code : int
        The original ADBC status code.
    vendor_code : int, optional
        A vendor-specific status code if present.
    sqlstate : str, optional
        The SQLSTATE code if present.
    """

    def __init__(self, message, *, status_code, vendor_code=None, sqlstate=None):
        super().__init__(message)
        self.status_code = status_code
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


cdef void check_error(AdbcStatusCode status, CAdbcError* error) except *:
    if status == ADBC_STATUS_OK:
        return

    message = AdbcStatusCodeMessage(status).decode("utf-8")
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
    elif status in (ADBC_STATUS_ALREADY_EXISTS, ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_UNAUTHENTICATED, ADBC_STATUS_UNAUTHORIZED):
        klass = ProgrammingError
    elif status == ADBC_STATUS_NOT_IMPLEMENTED:
        klass = NotSupportedError
    raise klass(message, status_code=status)


cdef CAdbcError empty_error():
    cdef CAdbcError error
    memset(&error, 0, cython.sizeof(error))
    return error


cdef class _AdbcHandle:
    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


cdef class AdbcDatabase(_AdbcHandle):
    cdef:
        CAdbcDatabase database

    def __init__(self, **kwargs) -> None:
        cdef CAdbcError c_error = empty_error()
        cdef AdbcStatusCode status
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
        if self.database.private_data == NULL:
            return

        cdef CAdbcError c_error = empty_error()
        cdef AdbcStatusCode status = AdbcDatabaseRelease(&self.database, &c_error)
        check_error(status, &c_error)


cdef class AdbcConnection(_AdbcHandle):
    cdef:
        CAdbcConnection connection

    def __init__(self, AdbcDatabase database, **kwargs) -> None:
        cdef CAdbcError c_error = empty_error()
        cdef AdbcStatusCode status
        cdef const char* c_key
        cdef const char* c_value
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

    def close(self) -> None:
        if self.connection.private_data == NULL:
            return

        cdef CAdbcError c_error = empty_error()
        cdef AdbcStatusCode status = AdbcConnectionRelease(&self.connection, &c_error)
        check_error(status, &c_error)


cdef class AdbcStatement(_AdbcHandle):
    cdef:
        CAdbcStatement statement

    def __init__(self, AdbcConnection connection) -> None:
        cdef CAdbcError c_error = empty_error()
        cdef const char* c_key
        cdef const char* c_value
        memset(&self.statement, 0, cython.sizeof(CAdbcStatement))

        status = AdbcStatementNew(&connection.connection, &self.statement, &c_error)
        check_error(status, &c_error)

    def bind(self, data) -> None:
        """
        Parameters
        ----------
        data : pyarrow.RecordBatch, pyarrow.RecordBatchReader, or pyarrow.Table
        """
        cdef CAdbcError c_error = empty_error()
        cdef CArrowArray c_array
        cdef CArrowSchema c_schema
        cdef CArrowArrayStream c_stream
        if isinstance(data, pyarrow.RecordBatch):
            data._export_to_c(<uintptr_t> &c_array, <uintptr_t>&c_schema)
            status = AdbcStatementBind(&self.statement, &c_array, &c_schema, &c_error)
        else:
            if isinstance(data, pyarrow.Table):
                # Table lacks the export function
                data = data.to_reader()
            elif not isinstance(data, pyarrow.RecordBatchReader):
                raise TypeError("data must be RecordBatch(Reader) or Table")
            data._export_to_c(<uintptr_t> &c_stream)
            status = AdbcStatementBindStream(&self.statement, &c_stream, &c_error)

        check_error(status, &c_error)

    def close(self) -> None:
        if self.statement.private_data == NULL:
            return

        cdef CAdbcError c_error = empty_error()
        cdef AdbcStatusCode status = AdbcStatementRelease(&self.statement, &c_error)
        check_error(status, &c_error)

    def execute(self) -> None:
        cdef CAdbcError c_error = empty_error()
        status = AdbcStatementExecute(&self.statement, &c_error)
        check_error(status, &c_error)

    def get_stream(self) -> pyarrow.RecordBatchReader:
        cdef CAdbcError c_error = empty_error()
        cdef CArrowArrayStream c_stream
        status = AdbcStatementGetStream(&self.statement, &c_stream, &c_error)
        check_error(status, &c_error)
        return pyarrow.RecordBatchReader._import_from_c(<uintptr_t> &c_stream)

    def prepare(self) -> None:
        cdef CAdbcError c_error = empty_error()
        status = AdbcStatementPrepare(&self.statement, &c_error)
        check_error(status, &c_error)

    def set_options(self, **kwargs) -> None:
        cdef CAdbcError c_error = empty_error()
        for key, value in kwargs.items():
            key = key.encode("utf-8")
            value = value.encode("utf-8")
            c_key = key
            c_value = value
            status = AdbcStatementSetOption(&self.statement, c_key, c_value, &c_error)
            check_error(status, &c_error)

    def set_sql_query(self, query: str) -> None:
        cdef CAdbcError c_error = empty_error()
        status = AdbcStatementSetSqlQuery(&self.statement, query.encode("utf-8"), &c_error)
        check_error(status, &c_error)
