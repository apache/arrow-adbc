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
# RECIPE KEYWORDS: bulk ingestion from PyArrow Dataset
# RECIPE STARTS HERE

#: ADBC makes it easy to load PyArrow datasets into your datastore.

import os
import tempfile
from pathlib import Path

import pyarrow
import pyarrow.csv
import pyarrow.dataset
import pyarrow.feather
import pyarrow.parquet

import adbc_driver_postgresql.dbapi

uri = os.environ["ADBC_POSTGRESQL_TEST_URI"]
conn = adbc_driver_postgresql.dbapi.connect(uri)

#: For the purposes of testing, we'll first make sure the tables we're about
#: to use don't exist.
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS csvtable")
    cur.execute("DROP TABLE IF EXISTS ipctable")
    cur.execute("DROP TABLE IF EXISTS pqtable")
    cur.execute("DROP TABLE IF EXISTS csvdataset")
    cur.execute("DROP TABLE IF EXISTS ipcdataset")
    cur.execute("DROP TABLE IF EXISTS pqdataset")

conn.commit()

#: Generating sample data
#: ~~~~~~~~~~~~~~~~~~~~~~

tempdir = tempfile.TemporaryDirectory(
    prefix="adbc-docs-",
    ignore_cleanup_errors=True,
)
root = Path(tempdir.name)
table = pyarrow.table(
    [
        [1, 1, 2],
        ["foo", "bar", "baz"],
    ],
    names=["ints", "strs"],
)

#: First we'll write single files.

csv_file = root / "example.csv"
pyarrow.csv.write_csv(table, csv_file)

ipc_file = root / "example.arrow"
pyarrow.feather.write_feather(table, ipc_file)

parquet_file = root / "example.parquet"
pyarrow.parquet.write_table(table, parquet_file)

#: We'll also generate some partitioned datasets.

csv_dataset = root / "csv_dataset"
pyarrow.dataset.write_dataset(
    table,
    csv_dataset,
    format="csv",
    partitioning=["ints"],
)

ipc_dataset = root / "ipc_dataset"
pyarrow.dataset.write_dataset(
    table,
    ipc_dataset,
    format="feather",
    partitioning=["ints"],
)

parquet_dataset = root / "parquet_dataset"
pyarrow.dataset.write_dataset(
    table,
    parquet_dataset,
    format="parquet",
    partitioning=["ints"],
)

#: Loading CSV Files into PostgreSQL
#: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#: We can directly pass a :py:class:`pyarrow.RecordBatchReader` (from
#: ``open_csv``) to ``adbc_ingest``.  We can also pass a
#: :py:class:`pyarrow.dataset.Dataset`, or a
#: :py:class:`pyarrow.dataset.Scanner`.

with conn.cursor() as cur:
    reader = pyarrow.csv.open_csv(csv_file)
    cur.adbc_ingest("csvtable", reader, mode="create")

    reader = pyarrow.dataset.dataset(
        csv_dataset,
        format="csv",
        partitioning=["ints"],
    )
    cur.adbc_ingest("csvdataset", reader, mode="create")

conn.commit()

with conn.cursor() as cur:
    cur.execute("SELECT ints, strs FROM csvtable ORDER BY ints, strs ASC")
    assert cur.fetchall() == [(1, "bar"), (1, "foo"), (2, "baz")]

    cur.execute("SELECT ints, strs FROM csvdataset ORDER BY ints, strs ASC")
    assert cur.fetchall() == [(1, "bar"), (1, "foo"), (2, "baz")]

#: Loading Arrow IPC (Feather) Files into PostgreSQL
#: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

with conn.cursor() as cur:
    reader = pyarrow.ipc.RecordBatchFileReader(ipc_file)
    #: Because of quirks in the PyArrow API, we have to read the file into
    #: memory.
    cur.adbc_ingest("ipctable", reader.read_all(), mode="create")

    #: The Dataset API will stream the data into memory and then into
    #: PostgreSQL, though.
    reader = pyarrow.dataset.dataset(
        ipc_dataset,
        format="feather",
        partitioning=["ints"],
    )
    cur.adbc_ingest("ipcdataset", reader, mode="create")

conn.commit()

with conn.cursor() as cur:
    cur.execute("SELECT ints, strs FROM ipctable ORDER BY ints, strs ASC")
    assert cur.fetchall() == [(1, "bar"), (1, "foo"), (2, "baz")]

    cur.execute("SELECT ints, strs FROM ipcdataset ORDER BY ints, strs ASC")
    assert cur.fetchall() == [(1, "bar"), (1, "foo"), (2, "baz")]

#: Loading Parquet Files into PostgreSQL
#: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

with conn.cursor() as cur:
    reader = pyarrow.parquet.ParquetFile(parquet_file)
    cur.adbc_ingest("pqtable", reader.iter_batches(), mode="create")

    reader = pyarrow.dataset.dataset(
        parquet_dataset,
        format="parquet",
        partitioning=["ints"],
    )
    cur.adbc_ingest("pqdataset", reader, mode="create")

conn.commit()

with conn.cursor() as cur:
    cur.execute("SELECT ints, strs FROM pqtable ORDER BY ints, strs ASC")
    assert cur.fetchall() == [(1, "bar"), (1, "foo"), (2, "baz")]

    cur.execute("SELECT ints, strs FROM pqdataset ORDER BY ints, strs ASC")
    assert cur.fetchall() == [(1, "bar"), (1, "foo"), (2, "baz")]

#: Cleanup
#: ~~~~~~~

conn.close()
tempdir.cleanup()
