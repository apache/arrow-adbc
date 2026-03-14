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
import platform
import re
import typing
import uuid

import pytest

import adbc_driver_manager.dbapi as dbapi

pytestmark = [pytest.mark.sqlite]


@pytest.fixture(scope="module", autouse=True)
def profile_dir(tmp_path_factory) -> typing.Generator[pathlib.Path, None, None]:
    path = tmp_path_factory.mktemp("profile_dir")
    with pytest.MonkeyPatch().context() as mp:
        mp.setenv("ADBC_PROFILE_PATH", str(path))
        yield path


@pytest.fixture(scope="module")
def sqlitedev(profile_dir) -> str:
    with (profile_dir / "sqlitedev.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"

[Options]
""")
    return "sqlitedev"


def test_profile_option(sqlitedev) -> None:
    # Test loading via "profile" option
    with dbapi.connect(profile=sqlitedev) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


def test_option_env_var(subtests, tmp_path, monkeypatch) -> None:
    # Test a profile that uses env var substitution for option values
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    # ruff: disable[E501]
    # fmt: off
    cases = [
        # env vars, raw string, rendered value

        # Make sure manager doesn't misinterpret empty string as null (there
        # is no null in TOML)
        ({}, "", ""),
        # No actual substitution
        ({}, "{{ env_var(NONEXISTENT)", "{{ env_var(NONEXISTENT)"),
        ({}, "{{ env_var(NONEXISTENT) }", "{{ env_var(NONEXISTENT) }"),
        ({}, "{ env_var(NONEXISTENT) }", "{ env_var(NONEXISTENT) }"),
        # Env var does not exist
        ({}, "{{ env_var(NONEXISTENT) }}", ""),
        ({}, "{{ env_var(NONEXISTENT) }}bar", "bar"),
        ({}, "foo{{ env_var(NONEXISTENT) }}", "foo"),
        ({}, "foo{{ env_var(NONEXISTENT) }}bar", "foobar"),
        # Multiple env vars do not exist
        ({}, "foo{{ env_var(NONEXISTENT) }}bar{{ env_var(NONEXISTENT2) }}baz", "foobarbaz"),
        # Multiple env vars do not exist in different positions
        ({}, "{{ env_var(NONEXISTENT) }}foobarbaz{{ env_var(NONEXISTENT2) }}", "foobarbaz"),

        # Check whitespace sensitivity
        ({"TESTVALUE": "a"}, "{{env_var(TESTVALUE)}},{{ env_var(TESTVALUE) }},{{env_var(TESTVALUE) }},{{ env_var(TESTVALUE)}},{{ env_var(TESTVALUE)     }}", "a,a,a,a,a"),

        # Multiple vars
        ({"TESTVALUE": "a", "TESTVALUE2": "b"}, "{{env_var(TESTVALUE)}}{{env_var(TESTVALUE2)}}", "ab"),
        ({"TESTVALUE": "a", "TESTVALUE2": "b"}, "foo{{env_var(TESTVALUE)}}bar{{env_var(TESTVALUE2)}}baz", "fooabarbbaz"),
    ]
    # fmt: on
    # ruff: enable[E501]

    for i, (env_vars, raw_value, rendered_value) in enumerate(cases):
        with subtests.test(i=i, msg=raw_value):
            with (tmp_path / "subst.toml").open("w") as sink:
                sink.write(f"""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
adbc.foo.bar = "{raw_value}"
""")

            with monkeypatch.context() as mp:
                for k, v in env_vars.items():
                    mp.setenv(k, v)

                expected = re.escape(
                    f"Unknown database option adbc.foo.bar='{rendered_value}'"
                )
                with pytest.raises(dbapi.Error, match=expected):
                    with dbapi.connect("profile://subst"):
                        pass


def test_option_env_var_multiple(tmp_path, monkeypatch) -> None:
    # Test that we can set multiple options via env var substitution
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))
    with (tmp_path / "subst.toml").open("w") as sink:
        # On Windows, ensure we get file:///c:/ not file://c:/
        windows = "/" if platform.system() == "Windows" else ""
        rest = "{{ env_var(TEST_DIR) }}/{{ env_var(TEST_NUM) }}"
        batch = "{{ env_var(BATCH_SIZE) }}"
        sink.write(f"""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
uri = "file://{windows}{rest}.db"
adbc.sqlite.query.batch_rows = "{batch}"
""")

    monkeypatch.setenv("TEST_DIR", str(tmp_path.as_posix()))
    monkeypatch.setenv("TEST_NUM", "42")
    monkeypatch.setenv("BATCH_SIZE", "1")

    key = "adbc.sqlite.query.batch_rows"
    with dbapi.connect("profile://subst") as conn:
        assert conn.adbc_database.get_option_int(key) == 1
        assert conn.adbc_connection.get_option_int(key) == 1

        assert conn.adbc_database.get_option("uri") == (tmp_path / "42.db").as_uri()

        with conn.cursor() as cursor:
            assert cursor.adbc_statement.get_option_int(key) == 1
            cursor.execute("CREATE TABLE foo (id INTEGER)")
            cursor.execute("INSERT INTO foo VALUES (1), (2)")
            cursor.execute("SELECT * FROM foo")
            with cursor.fetch_record_batch() as reader:
                assert len(next(reader)) == 1
                assert len(next(reader)) == 1


def test_option_env_var_invalid(subtests, tmp_path, monkeypatch) -> None:
    # Test that various invalid syntaxes fail
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    for contents, error in [
        ("{{ }}", "unsupported interpolation type in key `uri`: ``"),
        ("{{ bar_baz }}", "unsupported interpolation type in key `uri`: `bar_baz`"),
        ("{{ rand() }}", "unsupported interpolation type in key `uri`: `rand()`"),
        (
            "{{ env_var(TEST_DIR }}",
            "malformed env_var() in key `uri`: missing closing parenthesis",
        ),
        (
            "{{ env_var() }}",
            "malformed env_var() in key `uri`: missing environment variable name",
        ),
    ]:
        with (tmp_path / "subst.toml").open("w") as sink:
            sink.write(f"""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
uri = "{contents}"
    """)

        with subtests.test(msg=contents):
            with pytest.raises(
                dbapi.ProgrammingError, match=re.escape(f"In profile: {error}")
            ):
                with dbapi.connect("profile://subst"):
                    pass


@pytest.mark.xfail(reason="https://github.com/apache/arrow-adbc/issues/4086")
def test_option_override(tmp_path, monkeypatch) -> None:
    # Test that the driver is optional
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    with (tmp_path / "dev.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
adbc.sqlite.query.batch_rows = 7
""")

    key = "adbc.sqlite.query.batch_rows"
    with dbapi.connect("profile://dev") as conn:
        assert conn.adbc_database.get_option_int(key) == 7

    with dbapi.connect("profile://dev", db_kwargs={key: "42"}) as conn:
        assert conn.adbc_database.get_option_int(key) == 42


def test_uri(sqlitedev) -> None:
    # Test loading via profile:// URI
    with dbapi.connect(f"profile://{sqlitedev}") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


def test_driver_optional(subtests, tmp_path, monkeypatch) -> None:
    # Test that the driver is optional
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    with (tmp_path / "nodriver.toml").open("w") as sink:
        sink.write("""
profile_version = 1
[Options]
""")

    with subtests.test(msg="missing driver"):
        with pytest.raises(dbapi.ProgrammingError, match="Must set 'driver' option"):
            with dbapi.connect("profile://nodriver"):
                pass

    # TODO(https://github.com/apache/arrow-adbc/issues/4085): do we want to allow this?
    # with subtests.test(msg="uri"):
    #     with dbapi.connect("adbc_driver_sqlite", uri="profile://nodriver") as conn:
    #         with conn.cursor() as cursor:
    #             cursor.execute("SELECT sqlite_version()")
    #             assert cursor.fetchone() is not None

    with subtests.test(msg="profile"):
        with dbapi.connect("adbc_driver_sqlite", profile="nodriver") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT sqlite_version()")
                assert cursor.fetchone() is not None


# TODO(https://github.com/apache/arrow-adbc/issues/4085): do we want to allow this?

# @pytest.mark.xfail
# def test_driver_override(subtests, tmp_path, monkeypatch) -> None:
#     # Test that the driver can be overridden by an option
#     monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))
#     with subtests.test(msg="with override (URI)"):
#         with dbapi.connect("adbc_driver_sqlite", "profile://nonexistent") as conn:
#             with conn.cursor() as cursor:
#                 cursor.execute("SELECT sqlite_version()")
#                 assert cursor.fetchone() is not None

#     with subtests.test(msg="with override (profile)"):
#         with dbapi.connect("adbc_driver_sqlite", profile="nonexistent") as conn:
#             with conn.cursor() as cursor:
#                 cursor.execute("SELECT sqlite_version()")
#                 assert cursor.fetchone() is not None


def test_driver_invalid(subtests, tmp_path, monkeypatch) -> None:
    # Test invalid values for the driver
    # TODO(lidavidm): give a more specific error
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    with (tmp_path / "nodriver.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = 2
[Options]
""")

    with subtests.test(msg="numeric driver"):
        with pytest.raises(dbapi.ProgrammingError, match="Must set 'driver' option"):
            with dbapi.connect("profile://nodriver"):
                pass

    with (tmp_path / "nodriver.toml").open("w") as sink:
        sink.write("""
profile_version = 1
[driver]
foo = "bar"
[Options]
""")

    with subtests.test(msg="table driver"):
        with pytest.raises(dbapi.ProgrammingError, match="Must set 'driver' option"):
            with dbapi.connect("profile://nodriver"):
                pass


def test_version_invalid(tmp_path, monkeypatch) -> None:
    # Test that invalid versions are rejected
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    with (tmp_path / "badversion.toml").open("w") as sink:
        sink.write("""
driver = "adbc_driver_sqlite"
[Options]
""")
    with pytest.raises(
        dbapi.ProgrammingError, match="Profile version is not an integer"
    ):
        with dbapi.connect("profile://badversion"):
            pass

    with (tmp_path / "badversion.toml").open("w") as sink:
        sink.write("""
driver = "adbc_driver_sqlite"
profile_version = "1"
[Options]
""")
    with pytest.raises(
        dbapi.ProgrammingError, match="Profile version is not an integer"
    ):
        with dbapi.connect("profile://badversion"):
            pass

    with (tmp_path / "badversion.toml").open("w") as sink:
        sink.write("""
driver = "adbc_driver_sqlite"
profile_version = 9001
[Options]
""")
    with pytest.raises(
        dbapi.ProgrammingError, match="Profile version '9001' is not supported"
    ):
        with dbapi.connect("profile://badversion"):
            pass


def test_reject_malformed(tmp_path, monkeypatch) -> None:
    # Test that invalid profiles are rejected
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    with (tmp_path / "nodriver.toml").open("w") as sink:
        sink.write("""
profile_version = 1
[Options]
""")
    with pytest.raises(dbapi.ProgrammingError, match="Must set 'driver' option"):
        with dbapi.connect("profile://nodriver"):
            pass

    with (tmp_path / "nooptions.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
""")
    with pytest.raises(dbapi.ProgrammingError, match="Profile options is not a table"):
        with dbapi.connect("profile://nooptions"):
            pass

    with (tmp_path / "unknownkeys.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
[foobar]
""")
    # Unknown keys is OK, though
    with dbapi.connect("profile://unknownkeys"):
        pass


def test_driver_options(tmp_path, monkeypatch) -> None:
    # Test that options are properly applied
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))

    # On Windows, ensure we get file:///c:/ not file://c:/
    windows = "/" if platform.system() == "Windows" else ""
    uri = f"file://{windows}{tmp_path.resolve().absolute().as_posix()}/foo.db"
    with (tmp_path / "sqlitetest.toml").open("w") as sink:
        sink.write(f"""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
uri = "{uri}"
""")
    with dbapi.connect("profile://sqlitetest") as conn:
        assert conn.adbc_database.get_option("uri") == uri


def test_load_driver_manifest(tmp_path, monkeypatch) -> None:
    # Test a profile that references a manifest
    manifest_path = tmp_path / "manifest"
    profile_path = tmp_path / "profile"
    manifest_path.mkdir()
    profile_path.mkdir()
    monkeypatch.setenv("ADBC_DRIVER_PATH", str(manifest_path))
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(profile_path))

    with (manifest_path / "sqlitemanifest.toml").open("w") as sink:
        sink.write("""
manifest_version = 1
[Driver]
shared = "adbc_driver_sqlite"
""")

    with (profile_path / "proddata.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "sqlitemanifest"
[Options]
""")
    with dbapi.connect("profile://proddata") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


def test_subdir(monkeypatch, tmp_path) -> None:
    # Test that we can search in subdirectories
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))
    subdir = tmp_path / "sqlite" / "prod"
    subdir.mkdir(parents=True)

    with (subdir / "sqlitetest.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
""")
    with dbapi.connect("profile://sqlite/prod/sqlitetest") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


def test_absolute(monkeypatch, tmp_path) -> None:
    # Test that we can load profiles by absolute path
    monkeypatch.setenv("ADBC_PROFILE_PATH", str(tmp_path))
    subdir = tmp_path / "sqlite" / "staging"
    subdir.mkdir(parents=True)

    with (subdir / "sqlitetest.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
""")

    path = (subdir / "sqlitetest.toml").absolute().as_posix()
    with dbapi.connect(f"profile://{path}") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            assert cursor.fetchone() is not None


@pytest.mark.system
def test_user_path() -> None:
    if platform.system() == "Darwin":
        path = pathlib.Path.home() / "Library/Application Support/ADBC/Profiles"
    elif platform.system() == "Linux":
        path = pathlib.Path.home() / ".config/adbc/profiles"
    elif platform.system() == "Windows":
        path = pathlib.Path(os.environ["LOCALAPPDATA"]) / "ADBC/Profiles"
    else:
        pytest.skip(f"Unsupported platform {platform.system()}")

    subdir = str(uuid.uuid4())
    profile = str(uuid.uuid4())
    subpath = path / subdir
    path.mkdir(exist_ok=True, parents=True)
    subpath.mkdir(exist_ok=True)

    with (path / f"{profile}.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
""")

    with (subpath / f"{profile}.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
""")

    try:
        with dbapi.connect(f"profile://{profile}") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT sqlite_version()")
                assert cursor.fetchone() is not None

        with dbapi.connect(f"profile://{subdir}/{profile}") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT sqlite_version()")
                assert cursor.fetchone() is not None
    finally:
        (path / f"{profile}.toml").unlink()
        (subpath / f"{profile}.toml").unlink()
        subpath.rmdir()


def test_conda(conda_prefix) -> None:
    path = conda_prefix / "etc/adbc/profiles/"
    path.mkdir(exist_ok=True, parents=True)
    profile = str(uuid.uuid4())

    with (path / f"{profile}.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
""")
    try:
        with dbapi.connect(f"profile://{profile}") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT sqlite_version()")
                assert cursor.fetchone() is not None
    finally:
        (path / f"{profile}.toml").unlink()


def test_conda_subdir(conda_prefix) -> None:
    subdir = str(uuid.uuid4())
    path = conda_prefix / f"etc/adbc/profiles/{subdir}"
    path.mkdir(exist_ok=True, parents=True)
    profile = str(uuid.uuid4())

    with (path / f"{profile}.toml").open("w") as sink:
        sink.write("""
profile_version = 1
driver = "adbc_driver_sqlite"
[Options]
""")
    try:
        with dbapi.connect(f"profile://{subdir}/{profile}") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT sqlite_version()")
                assert cursor.fetchone() is not None
    finally:
        (path / f"{profile}.toml").unlink()
        path.rmdir()


# For virtualenv tests: see Compose job python-venv
