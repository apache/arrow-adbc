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

# RECIPE CATEGORY: driver manager
# RECIPE KEYWORDS: dynamic driver loading
# RECIPE STARTS HERE
#: While the DB-API_ bindings are recommended for general use, the low-level
#: bindings are also available.  These mostly mirror the ADBC C API directly.
#: They can be useful to opt out of some behaviors of the DB-API wrapper.
#:
#: .. _DB-API: https://peps.python.org/pep-0249/

import pyarrow

import adbc_driver_manager
import adbc_driver_sqlite

#: The driver packages do still have conveniences to create the root
#: :class:`AdbcDatabase <adbc_driver_manager.AdbcDatabase>` object.
db: adbc_driver_manager.AdbcDatabase = adbc_driver_sqlite.connect()
#: The database must then be wrapped in a :class:`AdbcConnection
#: <adbc_driver_manager.AdbcConnection>`.  This is similar in scope to the
#: DB-API :class:`Connection <adbc_driver_manager.dbapi.Connection>` class.
conn = adbc_driver_manager.AdbcConnection(db)
#: Finally, we can wrap the connection in a :class:`AdbcStatement
#: <adbc_driver_manager.AdbcStatement>`, which corresponds roughly to the
#: DB-API :class:`Cursor <adbc_driver_manager.dbapi.Cursor>` class.
stmt = adbc_driver_manager.AdbcStatement(conn)

#: Now we can directly set the query.  Unlike the regular DB-API bindings, this
#: will not prepare the statement.  (Depending on the driver, this may or may
#: not make a difference, especially if executing the same query multiple
#: times.)
stmt.set_sql_query("SELECT 1 AS THEANSWER")

#: When we execute the query, we get an `Arrow C Stream Interface`_ handle
#: (wrapped as a PyCapsule_) that we need to import using a library like
#: PyArrow_.
#:
#: .. _Arrow C Stream Interface:
#:    https://arrow.apache.org/docs/format/CStreamInterface.html
#: .. _PyArrow: https://pypi.org/project/pyarrow/
#: .. _PyCapsule: https://docs.python.org/3/c-api/capsule.html
handle, rowcount = stmt.execute_query()
#: The SQLite driver does not know the row count of the result set up front
#: (other drivers, like the PostgreSQL driver, may know).
assert rowcount == -1
#: We can use the PyArrow APIs to read the result.
reader = pyarrow.RecordBatchReader.from_stream(handle)
assert reader.schema == pyarrow.schema([("THEANSWER", "int64")])
#: Finally, we have to clean up all the objects.  (They also support the
#: context manager protocol.)
stmt.close()
conn.close()
db.close()
