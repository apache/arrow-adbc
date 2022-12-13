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

"""Low-level ADBC bindings for Python.

The root module provides a fairly direct, 1:1 mapping to the C API
definitions in Python.  For a higher-level interface, use
:mod:`adbc_driver_manager.dbapi`.  (This requires PyArrow.)
"""

from ._lib import (
    INGEST_OPTION_MODE,
    INGEST_OPTION_MODE_APPEND,
    INGEST_OPTION_MODE_CREATE,
    INGEST_OPTION_TARGET_TABLE,
    AdbcConnection,
    AdbcDatabase,
    AdbcInfoCode,
    AdbcStatement,
    AdbcStatusCode,
    ArrowArrayHandle,
    ArrowArrayStreamHandle,
    ArrowSchemaHandle,
    DatabaseError,
    DataError,
    Error,
    GetObjectsDepth,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from ._version import __version__

__all__ = [
    "__version__",
    "INGEST_OPTION_MODE",
    "INGEST_OPTION_MODE_APPEND",
    "INGEST_OPTION_MODE_CREATE",
    "INGEST_OPTION_TARGET_TABLE",
    "AdbcConnection",
    "AdbcDatabase",
    "AdbcInfoCode",
    "AdbcStatement",
    "AdbcStatusCode",
    "ArrowArrayHandle",
    "ArrowArrayStreamHandle",
    "ArrowSchemaHandle",
    "DatabaseError",
    "DataError",
    "Error",
    "GetObjectsDepth",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]
