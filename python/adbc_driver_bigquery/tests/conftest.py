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

import os

import pytest


@pytest.fixture(scope="session")
def bigquery_auth_type() -> str:
    bigquery_auth_type = os.environ.get("ADBC_BIGQUERY_AUTH_TYPE")
    if not bigquery_auth_type:
        pytest.skip("Set ADBC_BIGQUERY_AUTH_TYPE to run tests")
    return bigquery_auth_type


@pytest.fixture(scope="session")
def bigquery_credentials() -> str:
    auth_type = bigquery_auth_type()
    if auth_type == "adbc.bigquery.sql.auth_type.auth_bigquery":
        return ""
    bigquery_credentials = os.environ.get("ADBC_BIGQUERY_AUTH_CREDENTIALS")
    if not bigquery_credentials:
        pytest.skip("Set ADBC_BIGQUERY_AUTH_CREDENTIALS to run tests")
    return bigquery_credentials


@pytest.fixture(scope="session")
def bigquery_project_id() -> str:
    bigquery_project_id = os.environ.get("ADBC_BIGQUERY_PROJECT_ID")
    if not bigquery_project_id:
        pytest.skip("Set ADBC_BIGQUERY_PROJECT_ID to run tests")
    return bigquery_project_id
