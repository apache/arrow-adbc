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
# RECIPE KEYWORDS: bulk ingestion to temporary table
# RECIPE STARTS HERE
#: ADBC allows creating and appending to temporary tables as well.

import os

import pyarrow

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: For the purposes of testing, we'll first make sure the tables we're about
#: to use don't exist.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS example")

#: To create a temporary table, just specify the option "temporary".
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

with conn.cursor() as cur:
    cur.adbc_ingest("example", data, mode="create", temporary=True)

conn.commit()

#: After ingestion, we can fetch the result.
with conn.cursor() as cur:
    cur.execute("SELECT * FROM example")
    assert cur.fetchone() == (1,)
    assert cur.fetchone() == (2,)

    cur.execute("SELECT COUNT(*) FROM example")
    assert cur.fetchone() == (4,)

#: Temporary tables are separate from regular tables, even if they have the
#: same name.

with conn.cursor() as cur:
    cur.adbc_ingest("example", data.slice(0, 2), mode="create", temporary=False)

conn.commit()

with conn.cursor() as cur:
    #: Because we have two tables with the same name, we have to explicitly
    #: reference the normal temporary table here.
    cur.execute("SELECT COUNT(*) FROM public.example")
    assert cur.fetchone() == (2,)

    cur.execute("SELECT COUNT(*) FROM example")
    assert cur.fetchone() == (4,)

conn.close()

#: After closing the connection, the temporary table is implicitly dropped.
#: If we reconnect, the table won't exist; we'll see only the 'normal' table.

with adbc_driver_postgresql.dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM example")
        assert cur.fetchone() == (2,)

#: All the regular ingestion options apply to temporary tables, too.  See
#: :ref:`recipe-postgresql-create-append` for more examples.
