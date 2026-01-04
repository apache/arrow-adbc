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

#: The Flight SQL driver supports OAuth 2.0 authentication. This example shows
#: how to connect using the Client Credentials flow (RFC 6749), which is
#: suitable for machine-to-machine authentication without user interaction.

import os

import adbc_driver_flightsql.dbapi
from adbc_driver_flightsql import DatabaseOptions, OAuthFlowType

uri = os.environ["ADBC_TEST_FLIGHTSQL_URI"]
token_uri = os.environ["ADBC_OAUTH_TOKEN_URI"]
client_id = os.environ["ADBC_OAUTH_CLIENT_ID"]
client_secret = os.environ["ADBC_OAUTH_CLIENT_SECRET"]

#: Connect using OAuth 2.0 Client Credentials flow.
#: The driver will automatically obtain and refresh access tokens.

db_kwargs = {
    DatabaseOptions.OAUTH_FLOW.value: OAuthFlowType.CLIENT_CREDENTIALS.value,
    DatabaseOptions.OAUTH_TOKEN_URI.value: token_uri,
    DatabaseOptions.OAUTH_CLIENT_ID.value: client_id,
    DatabaseOptions.OAUTH_CLIENT_SECRET.value: client_secret,
    #: Optionally, request specific scopes
    # DatabaseOptions.OAUTH_SCOPE.value: "dremio.all",
}

#: For testing with self-signed certificates, skip TLS verification.
#: In production, you should provide proper TLS certificates.
if os.environ.get("ADBC_OAUTH_SKIP_VERIFY", "true").lower() in ("1", "true"):
    db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"

conn = adbc_driver_flightsql.dbapi.connect(uri, db_kwargs=db_kwargs)

#: We can then execute queries as usual.

with conn.cursor() as cur:
    cur.execute("SELECT 1")

    result = cur.fetchone()
    print(result)

conn.close()
