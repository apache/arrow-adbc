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

#: By default, the Flight SQL driver limits the size of incoming/outgoing
#: messages.  You might see an error like this if those limits are exceeded::
#:
#:     INTERNAL: [FlightSQL] grpc: received message larger than max
#:
#: These limits can be adjusted to avoid this.

import os

import adbc_driver_flightsql.dbapi
from adbc_driver_flightsql import DatabaseOptions

uri = os.environ["ADBC_SQLITE_FLIGHTSQL_URI"]

#: This query generates about 16 MiB per batch, which will trip the default
#: limit.

query = """
WITH RECURSIVE generate_series(value) AS (
  SELECT 1
  UNION ALL
  SELECT value + 1 FROM generate_series
   WHERE value + 1 <= 2048
)
SELECT printf('%.*c', 16384, 'x') FROM generate_series
"""

#: When we execute the query, we'll get an error.

conn = adbc_driver_flightsql.dbapi.connect(uri)
with conn.cursor() as cur:
    cur.execute(query)

    try:
        cur.fetchallarrow()
    except adbc_driver_flightsql.dbapi.InternalError:
        # This exception is expected.
        pass
    else:
        assert False, "Did not raise expected exception"

conn.close()

#: We can instead change the limit when connecting.

conn = adbc_driver_flightsql.dbapi.connect(
    uri,
    db_kwargs={DatabaseOptions.WITH_MAX_MSG_SIZE.value: "2147483647"},
)
with conn.cursor() as cur:
    cur.execute(query)

    assert len(cur.fetchallarrow()) == 2048

conn.close()
