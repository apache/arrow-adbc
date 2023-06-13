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
#: ADBC allows creating and appending to database tables using Arrow
#: tables.

import os

import pyarrow

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: For the purposes of testing, we'll first make sure the table
#: doesn't exist.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS example")

#: Now we can create the table.
with conn.cursor() as cur:
    data = pyarrow.table(
        [
            [1, 2, None, 4],
        ],
        schema=pyarrow.schema(
            [
                ("ints", "int32"),
            ]
        ),
    )
    cur.adbc_ingest("example", data, mode="create")

conn.commit()

#: After ingestion, we can fetch the result.
with conn.cursor() as cur:
    cur.execute("SELECT * FROM example")
    assert cur.fetchone() == (1,)
    assert cur.fetchone() == (2,)

    cur.execute("SELECT COUNT(*) FROM example")
    assert cur.fetchone() == (4,)

#: If we try to ingest again, it'll fail, because the table already
#: exists.
with conn.cursor() as cur:
    try:
        cur.adbc_ingest("example", data, mode="create")
    except conn.OperationalError:
        pass
    else:
        raise RuntimeError("Should have failed!")

#: Instead, we can append to the table.
with conn.cursor() as cur:
    cur.adbc_ingest("example", data, mode="append")

    cur.execute("SELECT COUNT(*) FROM example")
    assert cur.fetchone() == (8,)

conn.close()
