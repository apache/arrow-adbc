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

import pyarrow
import pytest

import adbc_driver_manager
import adbc_driver_db2


@pytest.fixture
def db2(db2_uri):
    with adbc_driver_db2.connect(db2_uri) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


def test_query_trivial(db2):
    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        stmt.set_sql_query("SELECT 1 AS val FROM SYSIBM.SYSDUMMY1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        table = reader.read_all()
        assert table.num_rows == 1
        assert table.num_columns == 1


def test_query_multiple_rows(db2):
    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        stmt.set_sql_query(
            "SELECT col FROM (VALUES 1, 2, 3) AS t(col)"
        )
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        table = reader.read_all()
        assert table.num_rows == 3


def test_query_types(db2):
    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        stmt.set_sql_query(
            "SELECT "
            "  CAST(42 AS INTEGER) AS int_val, "
            "  CAST(123456789 AS BIGINT) AS bigint_val, "
            "  CAST(3.14 AS DOUBLE) AS double_val, "
            "  CAST('hello' AS VARCHAR(50)) AS str_val "
            "FROM SYSIBM.SYSDUMMY1"
        )
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        table = reader.read_all()
        assert table.num_rows == 1
        assert table.num_columns == 4


def test_options(db2):
    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        stmt.set_options(
            **{
                adbc_driver_db2.StatementOptions.BATCH_ROWS.value: "1024",
            }
        )
        stmt.set_sql_query("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_get_table_types(db2):
    stream = db2.get_table_types()
    reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
    table = reader.read_all()
    assert table.num_rows >= 1
    types = table.column(0).to_pylist()
    assert "TABLE" in types


def test_connection_commit_rollback(db2):
    db2.set_autocommit(False)
    db2.commit()
    db2.rollback()
    db2.set_autocommit(True)


def test_ingest_round_trip(db2):
    table = pyarrow.table(
        {
            "ints": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
            "strs": pyarrow.array(["a", "bb", "ccc"], type=pyarrow.string()),
        }
    )

    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        try:
            stmt.set_sql_query('DROP TABLE "adbc_test_ingest"')
            stmt.execute_update()
        except adbc_driver_manager.ProgrammingError:
            pass

    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        stmt.set_options(
            **{
                adbc_driver_manager.StatementOptions.INGEST_TARGET_TABLE.value: "adbc_test_ingest",
                adbc_driver_manager.StatementOptions.INGEST_MODE.value: adbc_driver_manager.INGEST_OPTION_MODE_CREATE,
            }
        )
        stmt.bind_stream(table.to_reader())
        stmt.execute_update()

    try:
        with adbc_driver_manager.AdbcStatement(db2) as stmt:
            stmt.set_sql_query('SELECT * FROM "adbc_test_ingest" ORDER BY "ints"')
            stream, _ = stmt.execute_query()
            reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
            result = reader.read_all()
            assert result.num_rows == 3
            assert result.column("ints").to_pylist() == [1, 2, 3]
    finally:
        with adbc_driver_manager.AdbcStatement(db2) as stmt:
            stmt.set_sql_query('DROP TABLE "adbc_test_ingest"')
            stmt.execute_update()


def test_get_table_schema(db2):
    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        try:
            stmt.set_sql_query('DROP TABLE "adbc_test_schema"')
            stmt.execute_update()
        except adbc_driver_manager.ProgrammingError:
            pass
        stmt.set_sql_query(
            'CREATE TABLE "adbc_test_schema" '
            "(id INTEGER NOT NULL, name VARCHAR(100))"
        )
        stmt.execute_update()

    try:
        schema_handle = db2.get_table_schema(
            catalog=None, db_schema=None, table_name="adbc_test_schema"
        )
        schema = pyarrow.Schema._import_from_c(schema_handle.address)
        assert len(schema) == 2
    finally:
        with adbc_driver_manager.AdbcStatement(db2) as stmt:
            stmt.set_sql_query('DROP TABLE "adbc_test_schema"')
            stmt.execute_update()


def test_error_invalid_query(db2):
    with adbc_driver_manager.AdbcStatement(db2) as stmt:
        stmt.set_sql_query("SELECT * FROM nonexistent_table_xyz_adbc")
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            stmt.execute_query()


def test_version():
    assert adbc_driver_db2.__version__  # type:ignore


def test_connection_options_enum():
    assert adbc_driver_db2.ConnectionOptions.DATABASE.value == "adbc.db2.database"
    assert adbc_driver_db2.ConnectionOptions.HOSTNAME.value == "adbc.db2.hostname"


def test_statement_options_enum():
    assert adbc_driver_db2.StatementOptions.BATCH_ROWS.value == "adbc.db2.query.batch_rows"
