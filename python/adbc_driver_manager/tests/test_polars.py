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

polars = pytest.importorskip("polars")
polars.testing = pytest.importorskip("polars.testing")


@pytest.mark.sqlite
def test_query_fetch_polars(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1, 'foo' AS foo, 2.0")
        polars.testing.assert_frame_equal(
            cur.fetch_polars(),
            polars.DataFrame(
                {
                    "1": [1],
                    "foo": ["foo"],
                    "2.0": [2.0],
                }
            ),
        )
