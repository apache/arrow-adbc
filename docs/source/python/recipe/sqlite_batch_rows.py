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

# RECIPE CATEGORY: SQLite
# RECIPE KEYWORDS: batch size, type inference
# RECIPE STARTS HERE

#: The ADBC SQLite driver allows control over the size of batches in result
#: sets.  Because the driver performs type inference, this also controls how
#: many rows the driver will look at to figure out the type.  If you know your
#: result set has many NULL rows up front, you may consider increasing the
#: batch size so that the driver can infer the correct types.

import adbc_driver_sqlite.dbapi

conn = adbc_driver_sqlite.dbapi.connect()

#: First we'll set up a demo table with 1024 NULL values.

with conn.cursor() as cur:
    cur.execute("CREATE TABLE demo (val TEXT)")

    cur.execute(
        """
    WITH RECURSIVE series(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1
        FROM series
        WHERE n + 1 <= 1024
    )
    INSERT INTO demo (val)
    SELECT NULL
    FROM series
    """
    )

    cur.execute("INSERT INTO demo VALUES ('foo'), ('bar'), ('baz')")

#: If we query the table naively, we'll get an error, because the driver first
#: looks at the first 1024 values to determine the column type.  But since
#: every value is NULL, it falls back to the default type of int64, which poses
#: a problem when it then encounters a string in the next batch.

with conn.cursor() as cur:
    try:
        cur.execute("SELECT * FROM demo")
        print(cur.fetchallarrow().schema)
    except OSError as e:
        print(e)
        # Output:
        # [SQLite] Type mismatch in column 0: expected INT64 but got STRING/BINARY
    else:
        raise RuntimeError("Expected an error")

#: We can tell the driver to increase the batch size (and hence look at more
#: rows).

with conn.cursor() as cur:
    cur.adbc_statement.set_options(
        **{
            adbc_driver_sqlite.StatementOptions.BATCH_ROWS.value: 2048,
        }
    )
    cur.execute("SELECT * FROM demo")
    print(cur.fetchallarrow().schema)
    # Output:
    # val: string
