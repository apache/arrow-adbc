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

import os
import pathlib
import typing

import pytest

from adbc_driver_manager import dbapi


def pytest_addoption(parser):
    parser.addoption(
        "--run-system",
        action="store_true",
        default=False,
        help="Run tests that may modify global filesystem paths",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-system"):
        mark = pytest.mark.skip(reason="Needs --run-system")
        for item in items:
            if "system" in item.keywords:
                item.add_marker(mark)


@pytest.fixture
def sqlite() -> typing.Generator[dbapi.Connection, None, None]:
    with dbapi.connect(driver="adbc_driver_sqlite") as conn:
        yield conn


@pytest.fixture(scope="session")
def conda_prefix() -> pathlib.Path:
    if "CONDA_PREFIX" not in os.environ:
        pytest.skip("only runs in Conda environment")
    return pathlib.Path(os.environ["CONDA_PREFIX"])
