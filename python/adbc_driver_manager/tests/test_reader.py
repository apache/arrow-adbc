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

from adbc_driver_manager._lib import ArrowArrayStreamHandle
from adbc_driver_manager._reader import AdbcRecordBatchReader

schema = pyarrow.schema([("ints", "int32")])
batches = [
    pyarrow.record_batch([[1, 2, 3, 4]], schema=schema),
]


def _make_reader():
    original = pyarrow.RecordBatchReader.from_batches(schema, batches)
    exported = ArrowArrayStreamHandle()
    original._export_to_c(exported.address)
    return AdbcRecordBatchReader._import_from_c(exported.address)


def test_reader() -> None:
    wrapped = _make_reader()
    assert wrapped.read_next_batch() == batches[0]


def test_reader_error() -> None:
    schema = pyarrow.schema([("ints", "int32")])

    def batches():
        yield pyarrow.record_batch([[1, 2, 3, 4]], schema=schema)
        raise ValueError("foo")

    original = pyarrow.RecordBatchReader.from_batches(schema, batches())
    exported = ArrowArrayStreamHandle()
    original._export_to_c(exported.address)
    wrapped = AdbcRecordBatchReader._import_from_c(exported.address)

    assert wrapped.read_next_batch() is not None
    with pytest.raises(pyarrow.ArrowInvalid):
        wrapped.read_next_batch()


def test_reader_methods() -> None:
    with _make_reader() as reader:
        assert reader.read_all() == pyarrow.Table.from_batches(batches, schema)

    with _make_reader() as reader:
        assert reader.read_pandas() is not None

    with _make_reader() as reader:
        for batch in reader:
            assert batch == batches[0]

    with _make_reader() as reader:
        with pytest.raises(NotImplementedError):
            assert reader.read_next_batch_with_custom_metadata() is not None

    with _make_reader() as reader:
        with pytest.raises(NotImplementedError):
            for batch in reader.iter_batches_with_custom_metadata():
                assert batch == batches[0]

    with _make_reader() as reader:
        assert reader.schema == schema


def test_check_error_with_released_stream():
    """check_error must re-raise when c_stream.release is NULL."""
    from adbc_driver_manager._reader import _AdbcErrorHelper

    # Default-constructed helper has a zeroed c_stream (release == NULL)
    helper = _AdbcErrorHelper.__new__(_AdbcErrorHelper)

    err = ValueError("upstream error")
    with pytest.raises(ValueError, match="upstream error"):
        helper.check_error(err)

    err = pyarrow.ArrowInvalid("Invalid or unsupported format string: 'd:5,2,32'")
    with pytest.raises(pyarrow.ArrowInvalid, match="unsupported format string"):
        helper.check_error(err)
