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
# RECIPE KEYWORDS: statement options
# RECIPE STARTS HERE

#: The ADBC driver tries to execute queries with COPY by default since it is
#: faster for large result sets.  PostgreSQL does not support ``COPY`` for all
#: kinds of queries, however.  For example, ``SHOW`` queries will not work.
#: In this case, you can explicitly disable the ``COPY`` optimization.

import os

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: The option can be set when creating the cursor:

with conn.cursor(
    adbc_stmt_kwargs={
        adbc_driver_postgresql.StatementOptions.USE_COPY.value: False,
    }
) as cur:
    cur.execute("SHOW ALL")
    print(cur.fetch_arrow_table().schema)
    # Output:
    # name: string
    # setting: string
    # description: string

#: Or it can be set afterwards:

with conn.cursor() as cur:
    cur.adbc_statement.set_options(
        **{
            adbc_driver_postgresql.StatementOptions.USE_COPY.value: False,
        }
    )
    cur.execute("SHOW ALL")
    print(cur.fetch_arrow_table().schema)
    # Output:
    # name: string
    # setting: string
    # description: string

#: Without the option, the query fails as the driver attempts to execute the
#: query with ``COPY``:

with conn.cursor() as cur:
    try:
        cur.execute("SHOW ALL")
    except conn.Error:
        pass
    else:
        raise RuntimeError("Expected error")

conn.close()
