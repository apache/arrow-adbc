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

# RECIPE CATEGORY: Flight SQL
# RECIPE KEYWORDS: connecting to Dremio
# RECIPE STARTS HERE

#: Dremio requires a username and password.  To connect to a Flight SQL
#: service with authentication, provide the options at connection time.

import os

import adbc_driver_flightsql.dbapi
import adbc_driver_manager

uri = os.environ["ADBC_DREMIO_FLIGHTSQL_URI"]
username = os.environ["ADBC_DREMIO_FLIGHTSQL_USER"]
password = os.environ["ADBC_DREMIO_FLIGHTSQL_PASS"]
conn = adbc_driver_flightsql.dbapi.connect(
    uri,
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: username,
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: password,
    },
)

#: We can then execute a simple query.

with conn.cursor() as cur:
    cur.execute("SELECT 1")

    assert cur.fetchone() == (1,)

conn.close()
