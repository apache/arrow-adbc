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

# RECIPE CATEGORY: PostgreSQL
# RECIPE KEYWORDS: authentication
# RECIPE STARTS HERE
#: To connect to a PostgreSQL database, the username and password must
#: be provided in the URI.  For example,
#:
#: .. code-block:: text
#:
#:    postgresql://username:password@hostname:port/dbname
#:
#: See the `PostgreSQL documentation
#: <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
#: for full details.

import os

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

with conn.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchone())
    # Output: (1,)

conn.close()
