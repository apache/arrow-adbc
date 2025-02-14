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

# RECIPE CATEGORY: PostgreSQL
# RECIPE KEYWORDS: connection pooling
# RECIPE STARTS HERE

#: ADBC does not implement connection pooling, as this is not generally a
#: feature of DBAPI drivers.  Instead, use a third party connection pool
#: like the one built into SQLAlchemy_.
#:
#: .. _SQLAlchemy: https://docs.sqlalchemy.org/en/20/core/pooling.html

import os

import sqlalchemy.pool

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]

source = adbc_driver_postgresql.dbapi.connect(uri)
#: :meth:`adbc_driver_manager.dbapi.Connection.adbc_clone` opens a new
#: connection from an existing connection, sharing internal resources where
#: possible.  For example, the PostgreSQL driver will share the internal OID
#: cache, saving some overhead on connection.
pool = sqlalchemy.pool.QueuePool(source.adbc_clone, max_overflow=1, pool_size=2)

#: We can now get connections out of the pool; SQLAlchemy overrides
#: ``close()`` to return the connection to the pool.
#:
#: .. note:: SQLAlchemy's wrapper does not support the context manager
#:           protocol, unlike the underlying ADBC connection.

conn = pool.connect()

assert pool.checkedin() == 0
assert pool.checkedout() == 1

with conn.cursor() as cur:
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)

conn.close()

assert pool.checkedin() == 1
assert pool.checkedout() == 0

source.close()
