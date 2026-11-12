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

import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class SQLiteQuirks(model.DriverQuirks):
    name = "sqlite"
    driver = "adbc_driver_sqlite"
    driver_name = "ADBC SQLite Driver"
    vendor_name = "SQLite"
    vendor_version = re.compile(r"3\.\d+\.\d+")
    short_version = "3"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=True,
        get_objects=True,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        get_objects_constraints_unique=False,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=True,
        statement_bulk_ingest_temporary=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        current_catalog="main",
        current_schema="",
        secondary_catalog="secondary",
        secondary_catalog_schema="",
        supported_xdbc_fields=["xdbc_type_name", "xdbc_nullable", "xdbc_is_nullable"],
        quirk_foundry=False,
    )
    setup = model.DriverSetup(
        database={
            "uri": "file:adbc-validation?mode=memory&cache=shared",
        },
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        message = str(error).lower()
        return "no such table" in message and (
            table_name is None or table_name.lower() in message
        )

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="sqlite")

    def qualify_temp_table(self, cursor, name: str) -> str:
        return self.quote_identifier("temp", name)

    def drop_table(self, *, temporary: bool = False, **kwargs) -> str:
        if temporary:
            kwargs["catalog_name"] = "temp"
        return super().drop_table(**kwargs)

    @property
    def sample_ddl_constraints(self) -> list[str]:
        return [
            "CREATE TABLE constraint_primary (a INT PRIMARY KEY)",
            "CREATE TABLE constraint_primary_multi (a INT, b INT, PRIMARY KEY(b, a))",
            "CREATE TABLE constraint_primary_multi2 (a INT, b INT, PRIMARY KEY(a, b))",
            "CREATE TABLE constraint_foreign "
            "(a INT, b INT REFERENCES constraint_primary(a))",
            "CREATE TABLE constraint_foreign_multi (a INT, b INT, c INT, "
            "FOREIGN KEY(c, b) REFERENCES constraint_primary_multi2(a, b))",
            "CREATE TABLE constraint_unique (a INT UNIQUE, b INT, c INT, UNIQUE(c, b))",
            "CREATE TABLE constraint_check (a INT CHECK(a > 0), b INT, CHECK(a > b))",
        ]
