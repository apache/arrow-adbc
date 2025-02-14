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
# RECIPE KEYWORDS: bind parameters
# RECIPE STARTS HERE

#: ADBC allows using Python and Arrow values as bind parameters.
#: Right now, the PostgreSQL driver only supports bind parameters
#: for queries that don't generate result sets.

import os

import pyarrow

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: We'll create an example table to test.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS example")
    cur.execute("CREATE TABLE example (ints INT, bigints BIGINT)")

conn.commit()

#: We can bind Python values:
with conn.cursor() as cur:
    cur.executemany("INSERT INTO example VALUES ($1, $2)", [(1, 2), (3, 4)])

    cur.execute("SELECT SUM(ints) FROM example")
    assert cur.fetchone() == (4,)

#: .. note:: If you're used to the format-string style ``%s`` syntax that
#:           libraries like psycopg use for bind parameters, note that this
#:           is not supported—only the PostgreSQL-native ``$1`` syntax.

#: We can also bind Arrow values:
with conn.cursor() as cur:
    data = pyarrow.record_batch(
        [
            [5, 6],
            [7, 8],
        ],
        names=["$1", "$2"],
    )
    cur.executemany("INSERT INTO example VALUES ($1, $2)", data)

    cur.execute("SELECT SUM(ints) FROM example")
    assert cur.fetchone() == (15,)

conn.close()
