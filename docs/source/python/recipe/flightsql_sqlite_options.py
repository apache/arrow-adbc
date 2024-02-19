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

#: The Flight SQL driver supports various options.

import os

import adbc_driver_flightsql.dbapi
from adbc_driver_flightsql import ConnectionOptions, DatabaseOptions

uri = os.environ["ADBC_SQLITE_FLIGHTSQL_URI"]
#: We can enable cookie support, which some server implementations require.
conn = adbc_driver_flightsql.dbapi.connect(
    uri,
    db_kwargs={DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true"},
)

#: Other options are set on the connection or statement.

#: For example, we can add a custom header to all outgoing requests.
custom_header = f"{ConnectionOptions.RPC_CALL_HEADER_PREFIX.value}x-custom-header"
conn.adbc_connection.set_options(**{custom_header: "value"})

#: We can also set timeouts.  These are in floating-point seconds.
conn.adbc_connection.set_options(
    **{
        ConnectionOptions.TIMEOUT_FETCH.value: 30.0,
        ConnectionOptions.TIMEOUT_QUERY.value: 30.0,
        ConnectionOptions.TIMEOUT_UPDATE.value: 30.0,
    }
)

#: These options will apply to all cursors we create.

with conn.cursor() as cur:
    cur.execute("SELECT 1")

    assert cur.fetchone() == (1,)

conn.close()
