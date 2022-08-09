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

"""
PEP 249 (DB-API 2.0) API wrapper for the ADBC Driver Manager.
"""

import datetime
import functools
import time
import typing
import warnings
from typing import Any, Dict, List, Optional

import pyarrow

from . import _lib

if typing.TYPE_CHECKING:
    from typing import Self

# ----------------------------------------------------------
# Globals

#: The DB-API API level (2.0).
apilevel = "2.0"
#: The thread safety level (connections may not be shared).
threadsafety = 1
#: The parameter style (qmark). This is hardcoded, but actually
#: depends on the driver.
paramstyle = "qmark"

Warning = _lib.Warning
Error = _lib.Error
InterfaceError = _lib.InterfaceError
DatabaseError = _lib.DatabaseError
DataError = _lib.DataError
OperationalError = _lib.OperationalError
IntegrityError = _lib.IntegrityError
InternalError = _lib.InternalError
ProgrammingError = _lib.ProgrammingError
NotSupportedError = _lib.NotSupportedError

# ----------------------------------------------------------
# Types

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    # Standard implementations from PEP 249 itself
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


class _TypeSet(frozenset):
    """A set of PyArrow type IDs that compares equal to subsets of self."""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _TypeSet):
            return not (other - self)
        elif isinstance(other, pyarrow.DataType):
            return other.id in self
        return False


STRING = _TypeSet([pyarrow.string().id, pyarrow.large_string().id])
BINARY = _TypeSet({pyarrow.binary().id, pyarrow.large_binary().id})
NUMBER = _TypeSet(
    [
        pyarrow.int8().id,
        pyarrow.int16().id,
        pyarrow.int32().id,
        pyarrow.int64().id,
        pyarrow.uint8().id,
        pyarrow.uint16().id,
        pyarrow.uint32().id,
        pyarrow.uint64().id,
        pyarrow.float32().id,
        pyarrow.float64().id,
    ]
)
DATETIME = _TypeSet(
    [
        pyarrow.date32().id,
        pyarrow.date64().id,
        pyarrow.time32("s").id,
        pyarrow.time64("ns").id,
        pyarrow.timestamp("s").id,
    ]
)
ROWID = _TypeSet([pyarrow.int64().id])

# ----------------------------------------------------------
# Functions


def connect(
    *,
    driver: str,
    entrypoint: str,
    db_kwargs: Optional[Dict[str, str]] = None,
    conn_kwargs: Optional[Dict[str, str]] = None
) -> "Connection":
    """
    Connect to a database via ADBC.

    Parameters
    ----------
    driver
        The driver name. For example, "adbc_driver_sqlite" will
        attempt to load libadbc_driver_sqlite.so on Unix-like systems,
        and adbc_driver_sqlite.dll on Windows.
    entrypoint
        The driver-specific entrypoint.
    db_kwargs
        Key-value parameters to pass to the driver to initialize the
        database.
    conn_kwargs
        Key-value parameters to pass to the driver to initialize the
        connection.
    """
    db = None
    conn = None

    if db_kwargs is None:
        db_kwargs = {}
    if conn_kwargs is None:
        conn_kwargs = {}

    try:
        db = _lib.AdbcDatabase(driver=driver, entrypoint=entrypoint, **db_kwargs)
        conn = _lib.AdbcConnection(db, **conn_kwargs)
        return Connection(db, conn)
    except Exception:
        if conn:
            conn.close()
        if db:
            db.close()
        raise


# ----------------------------------------------------------
# Classes


class _Closeable:
    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class Connection(_Closeable):
    """
    A DB-API 2.0 (PEP 249) connection.

    Do not create this object directly; use connect().
    """

    # Optional extension: expose exception classes on Connection
    Warning = _lib.Warning
    Error = _lib.Error
    InterfaceError = _lib.InterfaceError
    DatabaseError = _lib.DatabaseError
    DataError = _lib.DataError
    OperationalError = _lib.OperationalError
    IntegrityError = _lib.IntegrityError
    InternalError = _lib.InternalError
    ProgrammingError = _lib.ProgrammingError
    NotSupportedError = _lib.NotSupportedError

    def __init__(self, db: _lib.AdbcDatabase, conn: _lib.AdbcConnection) -> None:
        self._db = db
        self._conn = conn

        try:
            self._conn.set_autocommit(False)
        except _lib.NotSupportedError:
            self._commit_supported = False
            warnings.warn(
                "Cannot disable autocommit; conn will not be DB-API 2.0 compliant",
                category=Warning,
            )
        else:
            self._commit_supported = True

    def close(self) -> None:
        """Close the connection."""
        self._conn.close()
        self._db.close()

    def commit(self) -> None:
        """Explicitly commit."""
        if self._commit_supported:
            self._conn.commit()

    def rollback(self) -> None:
        """Explicitly rollback."""
        if self._commit_supported:
            self._conn.rollback()

    def cursor(self) -> "Cursor":
        """Create a new cursor for querying the database."""
        return Cursor(self)


class Cursor(_Closeable):
    """
    A DB-API 2.0 (PEP 249) cursor.

    Do not create this object directly; use Connection.cursor().
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._stmt = _lib.AdbcStatement(conn._conn)
        self._last_query: Optional[str] = None
        self._results: Optional["_RowIterator"] = None
        self._arraysize = 1

    @property
    def arraysize(self) -> int:
        """The number of rows to fetch at a time with fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._arraysize = size

    @property
    def connection(self) -> Connection:
        """
        Get the connection associated with this cursor.

        This is an optional DB-API extension.
        """
        return self._conn

    @property
    def description(self) -> Optional[List[tuple]]:
        """The schema of the result set."""
        if self._results is None:
            return None
        return self._results.description

    @property
    def rowcount(self):
        """
        Get the row count of the result set.

        This is always -1 since ADBC returns results as a stream.
        """
        return -1

    @property
    def rownumber(self):
        if self._results is not None:
            return self._results.rownumber
        return None

    def callproc(self, procname, parameters):
        raise NotSupportedError("Cursor.callproc")

    def close(self):
        """Close the cursor and free resources."""
        if self._results is not None:
            self._results.close()
        self._stmt.close()

    def execute(self, operation, parameters=None) -> None:
        """Execute a query."""
        self._results = None
        if operation != self._last_query:
            self._last_query = operation
            self._stmt.set_sql_query(operation)
            self._stmt.prepare()

        if parameters:
            rb = pyarrow.record_batch(
                [
                    [
                        param_value,
                    ]
                    for param_value in parameters
                ],
                names=[str(i) for i in range(len(parameters))],
            )
            arr_handle = _lib.ArrowArrayHandle()
            sch_handle = _lib.ArrowSchemaHandle()
            rb._export_to_c(arr_handle.address, sch_handle.address)
            self._stmt.bind(arr_handle, sch_handle)

        self._stmt.execute()
        handle = self._stmt.get_stream()
        self._results = _RowIterator(
            pyarrow.RecordBatchReader._import_from_c(handle.address)
        )

    def executemany(self, operation, seq_of_parameters):
        self._results = None
        if operation != self._last_query:
            self._last_query = operation
            self._stmt.set_sql_query(operation)
            self._stmt.prepare()

        if seq_of_parameters:
            rb = pyarrow.record_batch(
                [
                    pyarrow.array([row[col_idx] for row in seq_of_parameters])
                    for col_idx in range(len(seq_of_parameters[0]))
                ],
                names=[str(i) for i in range(len(seq_of_parameters[0]))],
            )
        else:
            rb = pyarrow.record_batch([])

        arr_handle = _lib.ArrowArrayHandle()
        sch_handle = _lib.ArrowSchemaHandle()
        rb._export_to_c(arr_handle.address, sch_handle.address)
        self._stmt.bind(arr_handle, sch_handle)
        self._stmt.execute()
        # XXX: must step through results to fully execute query
        handle = self._stmt.get_stream()
        reader = pyarrow.RecordBatchReader._import_from_c(handle.address)
        reader.read_all()

    def fetchone(self) -> tuple:
        """Fetch one row of the result."""
        if self._results is None:
            raise ProgrammingError(
                "Cannot fetchone() before execute()",
                status_code=_lib.AdbcStatusCode.INVALID_STATE,
            )
        return self._results.fetchone()

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch some rows of the result."""
        if self._results is None:
            raise ProgrammingError(
                "Cannot fetchmany() before execute()",
                status_code=_lib.AdbcStatusCode.INVALID_STATE,
            )
        if size is None:
            size = self.arraysize
        return self._results.fetchmany(size)

    def fetchall(self) -> List[tuple]:
        """Fetch all rows of the result."""
        if self._results is None:
            raise ProgrammingError(
                "Cannot fetchall() before execute()",
                status_code=_lib.AdbcStatusCode.INVALID_STATE,
            )
        return self._results.fetchall()

    def fetchallarrow(self) -> pyarrow.Table:
        """
        Fetch all rows of the result as a PyArrow Table.

        This implements a similar API as turbodbc.
        """
        return self.fetch_arrow_table()

    def fetch_arrow_table(self) -> pyarrow.Table:
        """
        Fetch all rows of the result as a PyArrow Table.

        This implements a similar API as DuckDB.
        """
        if self._results is None:
            raise ProgrammingError(
                "Cannot fetch_df() before execute()",
                status_code=_lib.AdbcStatusCode.INVALID_STATE,
            )
        return self._results.fetch_arrow_table()

    def fetch_df(self):
        """
        Fetch all rows of the result as a Pandas DataFrame.

        This implements a similar API as DuckDB.
        """
        if self._results is None:
            raise ProgrammingError(
                "Cannot fetch_df() before execute()",
                status_code=_lib.AdbcStatusCode.INVALID_STATE,
            )
        return self._results.fetch_df()

    def next(self):
        """Fetch the next row, or raise StopIteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def nextset(self):
        raise NotSupportedError("Cursor.nextset")

    def setinputsizes(self, sizes):
        # Not used
        pass

    def setoutputsize(self, size, column=None):
        # Not used
        pass

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()


class _RowIterator(_Closeable):
    """Track state needed to iterate over the result set."""

    def __init__(self, reader: pyarrow.RecordBatchReader) -> None:
        self._reader = reader
        self._current_batch = None
        self._next_row = 0
        self._finished = False
        self.rownumber = 0

    def close(self) -> None:
        self._reader.close()

    @property
    def description(self) -> List[tuple]:
        return [
            (field.name, field.type, None, None, None, None, None)
            for field in self._reader.schema
        ]

    def fetchone(self):
        if self._current_batch is None or self._next_row >= len(self._current_batch):
            try:
                self._current_batch = self._reader.read_next_batch()
                self._next_row = 0
            except StopIteration:
                self._current_batch = None
                self._finished = True

        if self._finished:
            return None

        row = tuple(
            _convert_value(arr, row=self._next_row)
            for arr in self._current_batch.columns
        )
        self._next_row += 1
        self.rownumber += 1
        return row

    def fetchmany(self, size: int):
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetch_arrow_table(self):
        return self._reader.read_all()

    def fetch_df(self):
        return self._reader.read_pandas()


@functools.singledispatch
def _convert_value(arr: pyarrow.Array, *, row: int) -> Any:
    return arr[row].as_py()
