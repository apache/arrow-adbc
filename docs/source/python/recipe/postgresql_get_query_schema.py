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
# RECIPE KEYWORDS: get query result set schema
# RECIPE STARTS HERE

#: ADBC lets you get the schema of a result set, without executing the query.

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

expected = pyarrow.schema(
    [
        ("ints", "int32"),
        ("bigints", "int64"),
    ]
)

with conn.cursor() as cur:
    assert cur.adbc_execute_schema("SELECT * FROM example") == expected

    #: PostgreSQL doesn't know the type here, so it just returns a guess.
    assert cur.adbc_execute_schema("SELECT $1 AS res") == pyarrow.schema(
        [
            ("res", "string"),
        ]
    )

conn.close()
