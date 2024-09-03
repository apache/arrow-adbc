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

#: ADBC can be used with Polars_, a dataframe library written in Rust.  As per
#: its documentation:
#:
#:     If the backend supports returning Arrow data directly then this facility
#:     will be used to efficiently instantiate the DataFrame; otherwise, the
#:     DataFrame is initialised from row-wise data.
#:
#: Obviously, ADBC returns Arrow data directly, making ADBC and Polars a
#: natural fit for each other.
#:
#: .. _Polars: https://pola.rs/

import os

import polars as pl

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]

#: We'll use Polars to create a sample table with
#: :external:py:meth:`polars.DataFrame.write_database`.  We don't need
#: to open an ADBC connection ourselves with Polars.

data = pl.DataFrame(
    {
        "ints": [1, 2, None, 4],
        "strs": ["a", "b", "c", "d"],
    }
)
data.write_database("example", uri, engine="adbc", if_table_exists="replace")

#: After creating the table, we can use
#: :external:py:func:`polars.read_database_uri` to fetch the result.  Again,
#: we can just pass the URI and tell Polars to manage ADBC for us.

df = pl.read_database_uri("SELECT * FROM example WHERE ints > 1", uri, engine="adbc")

assert len(df) == 2
