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

#: ADBC is integrated into Pandas_, a popular dataframe library.  Pandas can
#: use ADBC to read tables in PostgreSQL databases.  Compared to using
#: SQLAlchemy or other options, using ADBC with Pandas can have better
#: performance, such as by avoiding excess conversions to and from Python
#: objects.
#:
#: .. _Pandas: https://pandas.pydata.org/

import os

import pandas
import pyarrow

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: For the purposes of testing, we'll first make sure the tables we're about
#: to use don't exist.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS example")

#: Then we'll use ADBC to create a sample table.  (ADBC is not currently
#: integrated into :external:py:meth:`pandas.DataFrame.to_sql`.)

data = pyarrow.Table.from_pydict(
    {
        "ints": [1, 2, None, 4],
        "strs": ["a", "b", "c", "d"],
    }
)

with conn.cursor() as cur:
    cur.adbc_ingest("example", data, mode="create")

conn.commit()

#: After creating the table, we can pass an ADBC connection to
#: :external:py:func:`pandas.read_sql` to fetch the result.

df = pandas.read_sql("SELECT * FROM example", conn)

assert len(df) == 4

conn.close()
