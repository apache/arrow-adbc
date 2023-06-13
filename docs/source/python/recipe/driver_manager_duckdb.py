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
#: The ADBC driver manager can load a driver from a shared library.
#: For drivers provided by the Arrow project, you don't need to worry
#: about this; the Python package will take care of this for you.
#: Other drivers may need configuration, though.  We'll use `DuckDB
#: <https://duckdb.org>`_ as an example.

import duckdb

import adbc_driver_manager.dbapi

#: The driver manager needs the path to the shared library.  It also
#: needs the name of the entrypoint function.  Both of these should be
#: found in the driver's documentation.
conn = adbc_driver_manager.dbapi.connect(
    driver=duckdb.__file__,
    entrypoint="duckdb_adbc_init",
)

#: Once we provide that, everything else about the connection is the
#: same as usual.

with conn.cursor() as cur:
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)

conn.close()
