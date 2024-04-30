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

#: To connect to an unsecured Flight SQL service, just provide the URI.

import os

import adbc_driver_flightsql.dbapi

uri = os.environ["ADBC_SQLITE_FLIGHTSQL_URI"]
conn = adbc_driver_flightsql.dbapi.connect(uri)

#: We can then execute a simple query.

with conn.cursor() as cur:
    cur.execute("SELECT 1")

    assert cur.fetchone() == (1,)

conn.close()
