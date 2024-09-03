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

# RECIPE STARTS HERE
#: The DBAPI bindings prepare all statements before execution, because of this
#: part of the `DB-API specification`_:
#:
#:     A reference to the operation will be retained by the cursor. If the same
#:     operation object is passed in again, then the cursor can optimize its
#:     behavior.
#:
#: However, you may want to prepare the statement yourself, mostly because this
#: will give you the schema of the parameters (if supported by the server).
#: This can be done with :meth:`Cursor.adbc_prepare
#: <adbc_driver_manager.dbapi.Cursor.adbc_prepare>`.
#:
#: We'll demo this with the SQLite driver, though other drivers also support
#: this.
#:
#: .. _DB-API specification: https://peps.python.org/pep-0249/#id19

import pyarrow

import adbc_driver_sqlite.dbapi

conn = adbc_driver_sqlite.dbapi.connect()

with conn.cursor() as cur:
    param_schema = cur.adbc_prepare("SELECT ? + 1")
    assert param_schema == pyarrow.schema([("0", "null")])

    #: Note that the type of the parameter here is NULL, because the driver
    #: does not know the exact type.

    #: If we now execute the same query with the parameter, the statement will
    #: not be prepared a second time.

    cur.execute("SELECT ? + 1", parameters=(1,))
    assert cur.fetchone() == (2,)

    cur.execute("SELECT ? + 1", parameters=(41,))
    assert cur.fetchone() == (42,)

conn.close()
