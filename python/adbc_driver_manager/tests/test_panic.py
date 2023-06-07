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

"""Tests for the panic behavior of the Go driver FFI wrapper."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.panicdummy

_LIB_ENV_VAR = "PANICDUMMY_LIBRARY_PATH"


@pytest.fixture(scope="module")
def libpath() -> str:
    if _LIB_ENV_VAR not in os.environ:
        pytest.skip(f"{_LIB_ENV_VAR} not specified", allow_module_level=True)
    return os.environ[_LIB_ENV_VAR]


def test_panic_close(libpath) -> None:
    env = os.environ.copy()
    env["PANICDUMMY_FUNC"] = "StatementClose"
    env["PANICDUMMY_MESSAGE"] = "Boo!"
    output = subprocess.run(
        [
            sys.executable,
            Path(__file__).parent / "panictest.py",
            libpath,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        encoding="utf-8",
    )
    assert output.returncode != 0
    assert "Go panic in PanicDummy driver" in output.stderr
    assert "Boo!" in output.stderr
    assert "Go panicked, driver is in unknown state" in output.stderr


def test_panic_execute(libpath) -> None:
    env = os.environ.copy()
    env["PANICDUMMY_FUNC"] = "StatementExecuteQuery"
    env["PANICDUMMY_MESSAGE"] = "Boo!"
    output = subprocess.run(
        [
            sys.executable,
            Path(__file__).parent / "panictest.py",
            libpath,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        encoding="utf-8",
    )
    assert output.returncode != 0
    assert "Go panic in PanicDummy driver" in output.stderr
    assert "Boo!" in output.stderr
    assert "Go panicked, driver is in unknown state" in output.stderr
