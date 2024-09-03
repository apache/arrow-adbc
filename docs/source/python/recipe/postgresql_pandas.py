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

#: ADBC is integrated into pandas_, a popular dataframe library.  Pandas can
#: use ADBC to exchange data with PostgreSQL and other databases.  Compared to
#: using SQLAlchemy or other options, using ADBC with pandas can have better
#: performance, such as by avoiding excess conversions to and from Python
#: objects.
#:
#: .. _pandas: https://pandas.pydata.org/

import os

import pandas as pd

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: We'll use :external:py:meth:`pd.DataFrame.to_sql <pandas.DataFrame.to_sql>`
#: to create a sample table.

data = pd.DataFrame(
    {
        "ints": [1, 2, None, 4],
        "strs": ["a", "b", "c", "d"],
    }
)
data.to_sql("example", conn, if_exists="replace")
conn.commit()

#: After creating the table, we can pass an ADBC connection and a SQL query to
#: :external:py:func:`pd.read_sql <pandas.read_sql>` to get the result set as a
#: pandas DataFrame.

df = pd.read_sql("SELECT * FROM example WHERE ints > 1", conn)

assert len(df) == 2

conn.close()

#: Compared to the ADBC interface, pandas offers a more convenient and higher
#: level API, especially for those already using pandas.
