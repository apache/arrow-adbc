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

"""Tests for bulk ingest functionality using GizmoSQL."""

import pyarrow as pa

import adbc_driver_manager

# Aliases for long constant names to keep lines under 88 chars
INGEST_TARGET_TABLE = adbc_driver_manager.StatementOptions.INGEST_TARGET_TABLE.value
INGEST_MODE = adbc_driver_manager.StatementOptions.INGEST_MODE.value


def test_ingest_create(gizmosql):
    """Test creating a new table via bulk ingest."""
    table_name = "test_ingest_create"

    # Create sample data
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            "value": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
        }
    )

    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        # Configure for bulk ingest - create mode
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.create",
            }
        )

        # Bind the data
        stmt.bind_stream(data.to_reader())

        # Execute the ingest
        rows_affected = stmt.execute_update()
        assert rows_affected == 3

    # Verify the data was ingested by querying it back
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_sql_query(f"SELECT * FROM {table_name} ORDER BY id")
        stream, _ = stmt.execute_query()
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        result = reader.read_all()

        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]


def test_ingest_append(gizmosql):
    """Test appending to an existing table via bulk ingest."""
    table_name = "test_ingest_append"

    # First create the table with initial data
    initial_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "value": pa.array([10, 20], type=pa.int32()),
        }
    )

    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.create",
            }
        )
        stmt.bind_stream(initial_data.to_reader())
        stmt.execute_update()

    # Now append more data
    append_data = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "value": pa.array([30, 40], type=pa.int32()),
        }
    )

    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.append",
            }
        )
        stmt.bind_stream(append_data.to_reader())
        rows_affected = stmt.execute_update()
        assert rows_affected == 2

    # Verify all data is present
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_sql_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        stream, _ = stmt.execute_query()
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        result = reader.read_all()
        assert result.column("cnt")[0].as_py() == 4


def test_ingest_replace(gizmosql):
    """Test replacing a table via bulk ingest."""
    table_name = "test_ingest_replace"

    # First create the table with initial data
    initial_data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
        }
    )

    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.create",
            }
        )
        stmt.bind_stream(initial_data.to_reader())
        stmt.execute_update()

    # Now replace with new data
    replace_data = pa.table(
        {
            "id": pa.array([100, 200], type=pa.int64()),
        }
    )

    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.replace",
            }
        )
        stmt.bind_stream(replace_data.to_reader())
        rows_affected = stmt.execute_update()
        assert rows_affected == 2

    # Verify only the new data is present
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_sql_query(f"SELECT * FROM {table_name} ORDER BY id")
        stream, _ = stmt.execute_query()
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        result = reader.read_all()
        assert result.num_rows == 2
        assert result.column("id").to_pylist() == [100, 200]


def test_ingest_create_append(gizmosql):
    """Test create_append mode - creates if not exists, appends if exists."""
    table_name = "test_ingest_create_append"

    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
        }
    )

    # First call should create the table
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.create_append",
            }
        )
        stmt.bind_stream(data.to_reader())
        rows_affected = stmt.execute_update()
        assert rows_affected == 2

    # Second call should append
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.create_append",
            }
        )
        stmt.bind_stream(data.to_reader())
        rows_affected = stmt.execute_update()
        assert rows_affected == 2

    # Verify we have 4 rows total
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_sql_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        stream, _ = stmt.execute_query()
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        result = reader.read_all()
        assert result.column("cnt")[0].as_py() == 4


def test_ingest_multiple_batches(gizmosql):
    """Test ingesting data with multiple record batches."""
    table_name = "test_ingest_multi_batch"

    # Create data with multiple batches
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.string()),
        ]
    )

    batches = [
        pa.record_batch(
            [
                pa.array([1, 2, 3], type=pa.int64()),
                pa.array(["a", "b", "c"], type=pa.string()),
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.array([4, 5], type=pa.int64()),
                pa.array(["d", "e"], type=pa.string()),
            ],
            schema=schema,
        ),
    ]

    reader = pa.RecordBatchReader.from_batches(schema, batches)

    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_options(
            **{
                INGEST_TARGET_TABLE: table_name,
                INGEST_MODE: "adbc.ingest.mode.create",
            }
        )
        stmt.bind_stream(reader)
        rows_affected = stmt.execute_update()
        assert rows_affected == 5

    # Verify all data was ingested
    with adbc_driver_manager.AdbcStatement(gizmosql) as stmt:
        stmt.set_sql_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        stream, _ = stmt.execute_query()
        reader = pa.RecordBatchReader._import_from_c(stream.address)
        result = reader.read_all()
        assert result.column("cnt")[0].as_py() == 5


def test_ingest_via_dbapi_with_reader(gizmosql_dbapi):
    """Test bulk ingest using the DBAPI interface with RecordBatchReader."""
    table_name = "test_ingest_dbapi_reader"

    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["x", "y", "z"], type=pa.string()),
        }
    )

    # Test using the cursor's adbc_ingest method with explicit reader
    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, data.to_reader())
        assert rows_affected == 3

    # Verify via DBAPI query
    with gizmosql_dbapi.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cur.fetchone()
        assert result[0] == 3


def test_ingest_via_dbapi_with_table(gizmosql_dbapi):
    """Test bulk ingest using the DBAPI interface with Table directly."""
    table_name = "test_ingest_dbapi_table"

    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["x", "y", "z"], type=pa.string()),
        }
    )

    # Test using the cursor's adbc_ingest method with Table directly
    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, data)
        assert rows_affected == 3

    # Verify via DBAPI query
    with gizmosql_dbapi.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cur.fetchone()
        assert result[0] == 3


def test_ingest_dbapi_modes(gizmosql_dbapi):
    """Test different ingest modes via DBAPI (append, replace, create_append)."""
    table_name = "test_ingest_dbapi_modes"

    # Create initial table
    initial_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
        }
    )

    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, initial_data, mode="create")
        assert rows_affected == 2

    # Test append mode
    append_data = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
        }
    )

    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, append_data, mode="append")
        assert rows_affected == 2

    # Verify 4 rows total
    with gizmosql_dbapi.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cur.fetchone()[0] == 4

    # Test replace mode
    replace_data = pa.table(
        {
            "id": pa.array([100, 200, 300], type=pa.int64()),
        }
    )

    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, replace_data, mode="replace")
        assert rows_affected == 3

    # Verify 3 rows (replaced)
    with gizmosql_dbapi.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cur.fetchone()[0] == 3


def test_ingest_dbapi_create_append(gizmosql_dbapi):
    """Test create_append mode via DBAPI."""
    table_name = "test_ingest_dbapi_create_append"

    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
        }
    )

    # First call should create the table
    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, data, mode="create_append")
        assert rows_affected == 2

    # Second call should append to existing table
    with gizmosql_dbapi.cursor() as cur:
        rows_affected = cur.adbc_ingest(table_name, data, mode="create_append")
        assert rows_affected == 2

    # Verify 4 rows total
    with gizmosql_dbapi.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cur.fetchone()[0] == 4
