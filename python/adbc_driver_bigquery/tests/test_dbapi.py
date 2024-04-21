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

import pytest
from adbc_driver_bigquery import DatabaseOptions, dbapi


@pytest.fixture
def bigquery(
    bigquery_auth_type: str, bigquery_credentials: str, bigquery_project_id: str
):
    db_kwargs = {
        DatabaseOptions.AUTH_TYPE.value: bigquery_auth_type,
        DatabaseOptions.AUTH_CREDENTIALS.value: bigquery_credentials,
        DatabaseOptions.PROJECT_ID.value: bigquery_project_id,
    }
    with dbapi.connect(db_kwargs) as conn:
        yield conn


def test_query_trivial(bigquery):
    with bigquery.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
