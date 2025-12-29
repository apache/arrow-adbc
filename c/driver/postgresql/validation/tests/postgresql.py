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


class PostgreSQLQuirks(model.DriverQuirks):
    name = "postgresql"
    driver = "adbc_driver_postgresql"
    driver_name = "ADBC PostgreSQL Driver"
    vendor_name = "PostgreSQL"
    vendor_version = re.compile(r"18[0-9]{4}")
    short_version = "18"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=True,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_rows_affected=True,
        current_catalog="postgres",
        current_schema="public",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("ADBC_POSTGRESQL_TEST_URI"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def bind_parameter(self, index: int) -> str:
        """PostgreSQL uses $1, $2, $3, etc. for parameter placeholders."""
        return f"${index}"

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        """Check if the error indicates a table not found condition."""
        error_str = str(error).lower()
        return (
            (
                "relation" in error_str
                and "does not exist" in error_str
                and table_name.lower() in error_str
            )
            or (
                "table" in error_str
                and "does not exist" in error_str
                and table_name.lower() in error_str
            )
            or ("undefined_table" in error_str and table_name.lower() in error_str)
        )

    def quote_one_identifier(self, identifier: str) -> str:
        """Quote an identifier using PostgreSQL's double-quote syntax."""
        identifier = identifier.replace('"', '""')
        return f'"{identifier}"'

    def split_statement(self, statement: str) -> list[str]:
        """Split a multi-statement SQL string into individual statements."""
        return quirks.split_statement(statement)


QUIRKS = [PostgreSQLQuirks()]
