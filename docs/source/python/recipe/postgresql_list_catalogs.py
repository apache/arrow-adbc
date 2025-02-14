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
# RECIPE KEYWORDS: query catalog
# RECIPE STARTS HERE

#: ADBC allows listing tables, catalogs, and schemas in the database.

import os

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: We'll create an example table to look for.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS example")
    cur.execute("CREATE TABLE example (ints INT, bigints BIGINT)")

conn.commit()

#: The data is given as a PyArrow RecordBatchReader.
objects = conn.adbc_get_objects(depth="all").read_all()

#: We'll convert it to plain Python data for convenience.
objects = objects.to_pylist()
catalog = objects[0]
assert catalog["catalog_name"] == "postgres"

db_schema = catalog["catalog_db_schemas"][0]
assert db_schema["db_schema_name"] == "public"

tables = db_schema["db_schema_tables"]
example = [table for table in tables if table["table_name"] == "example"]
assert len(example) == 1
example = example[0]

assert example["table_columns"][0]["column_name"] == "ints"
assert example["table_columns"][1]["column_name"] == "bigints"

conn.close()
