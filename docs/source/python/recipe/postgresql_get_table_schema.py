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

#: ADBC lets you get the schema of a table as an Arrow schema.

import os

import pyarrow

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: We'll create some example tables to test.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS example")
    cur.execute("CREATE TABLE example (ints INT, bigints BIGINT)")

    cur.execute("CREATE SCHEMA IF NOT EXISTS other_schema")
    cur.execute("DROP TABLE IF EXISTS other_schema.example")
    cur.execute("CREATE TABLE other_schema.example (strings TEXT, values INT)")

conn.commit()

#: By default the "active" catalog/schema are assumed.
assert conn.adbc_get_table_schema("example") == pyarrow.schema(
    [
        ("ints", "int32"),
        ("bigints", "int64"),
    ]
)

#: We can explicitly specify the PostgreSQL schema to get the Arrow schema of
#: a table in a different namespace.
#:
#: .. note:: In PostgreSQL, you can only query the database (catalog) that you
#:           are connected to.  So we cannot specify the catalog here (or
#:           rather, there is no point in doing so).
#:
#: Note that the NUMERIC column is read as a string, because PostgreSQL
#: decimals do not map onto Arrow decimals.
assert conn.adbc_get_table_schema(
    "example",
    db_schema_filter="other_schema",
) == pyarrow.schema(
    [
        ("strings", "string"),
        ("values", "int32"),
    ]
)

conn.close()
