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

import abc
import asyncio
import itertools
import os

import asyncpg
import duckdb
import pandas
import psycopg
import sqlalchemy

import adbc_driver_postgresql.dbapi


class BenchmarkBase(abc.ABC):
    async_conn: asyncpg.Connection
    async_runner: asyncio.Runner
    conn: adbc_driver_postgresql.dbapi.Connection
    duck: duckdb.DuckDBPyConnection
    sqlalchemy_connection: sqlalchemy.engine.base.Connection

    def setup(self, *args, **kwargs) -> None:
        self.uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]

        self.table = self._make_table_name(*args, **kwargs)

        self.async_runner = asyncio.Runner()
        self.async_conn = self.async_runner.run(asyncpg.connect(dsn=self.uri))

        self.conn = adbc_driver_postgresql.dbapi.connect(self.uri)

        self.duck = duckdb.connect()
        self.duck.sql("INSTALL postgres_scanner")
        self.duck.sql("LOAD postgres_scanner")
        self.duck.sql(f"CALL postgres_attach('{self.uri}')")

        uri = self.uri.replace("postgres://", "postgresql+psycopg2://")
        self.sqlalchemy_connection = sqlalchemy.create_engine(uri).connect()

    def teardown(self, *args, **kwargs) -> None:
        self.async_runner.close()
        self.conn.close()
        self.sqlalchemy_connection.close()

    @abc.abstractmethod
    def _make_table_name(self, *args, **kwargs) -> str: ...

    def time_pandas_adbc(self, row_count: int, data_type: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.table}")
            cursor.fetch_df()

    def time_pandas_asyncpg(self, row_count: int, data_type: str) -> None:
        records = self.async_runner.run(
            self.async_conn.fetch(f"SELECT * FROM {self.table}")
        )
        pandas.DataFrame(records)

    # TODO: fails with 'undefined symbol' (probably need to get it into Conda)
    # def time_pandas_pgeon(self, row_count: int) -> None:
    #     pgeon.copy_query(self.uri, f"SELECT * FROM {self.table}").to_pandas()

    def time_pandas_psycopg2(self, row_count: int, data_type: str) -> None:
        pandas.read_sql_table(self.table, self.sqlalchemy_connection)

    def time_pandas_duckdb(self, row_count: int, data_type: str) -> None:
        self.duck.sql(f"SELECT * FROM {self.table}").fetchdf()


class OneColumnSuite(BenchmarkBase):
    """Benchmark the time it takes to fetch a single column of a given type."""

    SETUP_QUERIES = [
        "DROP TABLE IF EXISTS {table_name}",
        "CREATE TABLE {table_name} (items {data_type})",
        """INSERT INTO {table_name} (items)
        SELECT generated :: {data_type}
        FROM GENERATE_SERIES(1, {row_count}) temp(generated)""",
        # TODO: does an index matter, do we want to force PostgreSQL
        # to update statistics?
    ]

    param_data = {
        "row_count": [10_000, 100_000, 1_000_000],
        "data_type": ["INT", "BIGINT", "FLOAT", "DOUBLE PRECISION"],
    }

    param_names = list(param_data.keys())
    params = list(param_data.values())

    def setup_cache(self) -> None:
        self.uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
        with psycopg.connect(self.uri) as conn:
            with conn.cursor() as cursor:
                for row_count, data_type in itertools.product(*self.params):
                    table_name = self._make_table_name(row_count, data_type)
                    for query in self.SETUP_QUERIES:
                        cursor.execute(
                            query.format(
                                table_name=table_name,
                                row_count=row_count,
                                data_type=data_type,
                            )
                        )

    def _make_table_name(self, row_count: int, data_type: str) -> str:
        return (f"bench_{row_count}_{data_type.replace(' ', '_')}").lower()


class MultiColumnSuite(BenchmarkBase):
    """Benchmark the time it takes to fetch multiple columns of a given type."""

    SETUP_QUERIES = [
        "DROP TABLE IF EXISTS {table_name}",
        """
        CREATE TABLE {table_name} (
            a {data_type},
            b {data_type},
            c {data_type},
            d {data_type}
        )
        """,
        """
        INSERT INTO {table_name} (a, b, c, d)
        SELECT generated :: {data_type},
               generated :: {data_type},
               generated :: {data_type},
               generated :: {data_type}
        FROM GENERATE_SERIES(1, {row_count}) temp(generated)
        """,
    ]

    param_data = {
        "row_count": [10_000, 100_000, 1_000_000],
        "data_type": ["INT", "BIGINT", "FLOAT", "DOUBLE PRECISION"],
    }

    param_names = list(param_data.keys())
    params = list(param_data.values())

    def setup_cache(self) -> None:
        self.uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
        with psycopg.connect(self.uri) as conn:
            with conn.cursor() as cursor:
                for row_count, data_type in itertools.product(*self.params):
                    table_name = self._make_table_name(row_count, data_type)
                    for query in self.SETUP_QUERIES:
                        cursor.execute(
                            query.format(
                                table_name=table_name,
                                row_count=row_count,
                                data_type=data_type,
                            )
                        )

    def _make_table_name(self, row_count: int, data_type: str) -> str:
        return (f"bench_{row_count}_{data_type.replace(' ', '_')}").lower()
