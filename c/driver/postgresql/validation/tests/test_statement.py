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

import adbc_driver_manager

from adbc_drivers_validation import model
from adbc_drivers_validation.tests.statement import (
    TestStatement as BaseTestStatement,
    generate_tests,
)

from . import postgresql


class TestStatement(BaseTestStatement):
    """PostgreSQL-specific statement tests with overrides."""

    def test_rows_affected(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        """Override to accept -1 for CREATE TABLE"""
        table_name = "test_rows_affected"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            cursor.adbc_statement.set_sql_query(f"CREATE TABLE {table_name} (id INT)")
            rows_affected = cursor.adbc_statement.execute_update()

            # PostgreSQL returns -1 for CREATE TABLE
            assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {table_name} (id) VALUES (1)"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"UPDATE {table_name} SET id = id + 1 WHERE id = 1"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"DELETE FROM {table_name} WHERE id = 2"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(postgresql.QUIRKS, metafunc)
