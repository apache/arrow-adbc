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

#: The Flight SQL driver supports OAuth 2.0 Token Exchange (RFC 8693). This
#: allows exchanging an existing token (e.g., a JWT from an identity provider)
#: for a new token that can be used to access the Flight SQL service.

import os

import adbc_driver_flightsql.dbapi
from adbc_driver_flightsql import DatabaseOptions, OAuthFlowType, OAuthTokenType

uri = os.environ["ADBC_TEST_FLIGHTSQL_URI"]
token_uri = os.environ["ADBC_OAUTH_TOKEN_URI"]
#: This is typically a JWT or other token from your identity provider
subject_token = os.environ["ADBC_OAUTH_SUBJECT_TOKEN"]

#: For testing with self-signed certificates, skip TLS verification.
#: In production, you should provide proper TLS certificates.
db_kwargs = {}
if os.environ.get("ADBC_OAUTH_SKIP_VERIFY", "true").lower() in ("1", "true"):
    db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"

#: Connect using OAuth 2.0 Token Exchange flow.
#: The driver will exchange the subject token for an access token.

db_kwargs.update(
    {
        DatabaseOptions.OAUTH_FLOW.value: OAuthFlowType.TOKEN_EXCHANGE.value,
        DatabaseOptions.OAUTH_TOKEN_URI.value: token_uri,
        DatabaseOptions.OAUTH_EXCHANGE_SUBJECT_TOKEN.value: subject_token,
        #: Specify the type of the subject token being exchanged
        DatabaseOptions.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.value: (
            OAuthTokenType.JWT.value
        ),
        #: Optionally, specify the type of token you want to receive
        # DatabaseOptions.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE.value:
        #   OAuthTokenType.ACCESS_TOKEN.value,
        #: Optionally, specify the intended audience
        # DatabaseOptions.OAUTH_EXCHANGE_AUD.value: "my-service",
    }
)

conn = adbc_driver_flightsql.dbapi.connect(uri, db_kwargs=db_kwargs)

#: We can then execute queries as usual.

with conn.cursor() as cur:
    cur.execute("SELECT 1")

    result = cur.fetchone()
    print(result)

conn.close()
