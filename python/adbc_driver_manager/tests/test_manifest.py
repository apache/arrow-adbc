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

import pytest

import adbc_driver_manager.dbapi


@pytest.mark.sqlite
def test_manifest_indirect(tmp_path, monkeypatch) -> None:
    with (tmp_path / "testdriver.toml").open("w") as sink:
        sink.write(
            """
name = "my driver"
version = "0.1.0"

[Driver]
shared = "adbc_driver_sqlite"
            """
        )

    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    with adbc_driver_manager.dbapi.connect(driver="testdriver") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


@pytest.mark.sqlite
def test_manifest_implicit_uri(tmp_path, monkeypatch) -> None:
    with (tmp_path / "testdriver.toml").open("w") as sink:
        sink.write(
            """
name = "my driver"
version = "0.1.0"

[Driver]
shared = "adbc_driver_sqlite"
            """
        )

    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    with adbc_driver_manager.dbapi.connect(uri="sqlite:file::memory:") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


def test_manifest_indirect_unknown_driver(tmp_path, monkeypatch) -> None:
    with (tmp_path / "testdriver2.toml").open("w") as sink:
        sink.write(
            """
name = "my driver"
version = "0.1.0"

[Driver]
shared = "adbc_driver_goosedb"
            """
        )

    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    with pytest.raises(
        adbc_driver_manager.dbapi.Error, match="adbc_driver_goosedb"
    ) as excinfo:
        with adbc_driver_manager.dbapi.connect(driver="testdriver2"):
            pass

    assert "ADBC_DRIVER_PATH: " + str(tmp_path) in str(excinfo.value)
    assert "found {}".format(str(tmp_path / "testdriver2.toml")) in str(excinfo.value)


def test_manifest_indirect_missing_platform(tmp_path, monkeypatch) -> None:
    with (tmp_path / "testdriver3.toml").open("w") as sink:
        sink.write(
            """
name = "my driver"
version = "0.1.0"

[Driver.shared]
msdos_itanium64 = "adbc_driver_sqlite"
            """
        )

    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    with pytest.raises(adbc_driver_manager.dbapi.Error) as excinfo:
        with adbc_driver_manager.dbapi.connect(driver="testdriver3"):
            pass

    assert "ADBC_DRIVER_PATH: " + str(tmp_path) in str(excinfo.value)
    assert "found {}".format(str(tmp_path / "testdriver3.toml")) in str(excinfo.value)
    assert "Architectures found: msdos_itanium64" in str(excinfo.value)


def test_manifest_bad(tmp_path, monkeypatch) -> None:
    with (tmp_path / "testdriver4.toml").open("w") as sink:
        sink.write(
            """
name = "my driver"
version = "0.1.0"
            """
        )

    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    with pytest.raises(
        adbc_driver_manager.dbapi.Error, match="Driver path not defined in manifest"
    ):
        with adbc_driver_manager.dbapi.connect(driver="testdriver4"):
            pass
