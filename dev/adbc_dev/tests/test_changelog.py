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

from pathlib import Path

import pygit2
import pytest

from .. import changelog, title_check

root = Path(__file__).parent.parent.parent.parent.resolve()


@pytest.fixture(scope="module")
def repo() -> pygit2.Repository:
    repo_root = Path(__file__).parent.parent.parent.parent.resolve()
    return pygit2.Repository(repo_root)


@pytest.mark.parametrize("commit_type", title_check.COMMIT_TYPES)
def test_title_check_basic(commit_type) -> None:
    title = f"{commit_type}: test"
    commit = title_check.matches_commit_format(root, title)
    assert not commit.failed_validation_reasons
    assert commit.category == commit_type
    assert commit.components == []
    assert not commit.breaking_change
    assert commit.subject == "test"


@pytest.mark.parametrize("commit_type", title_check.COMMIT_TYPES)
def test_title_check_breaking(commit_type) -> None:
    title = f"{commit_type}!: test"
    commit = title_check.matches_commit_format(root, title)
    assert not commit.failed_validation_reasons
    assert commit.category == commit_type
    assert commit.components == []
    assert commit.breaking_change
    assert commit.subject == "test"


@pytest.mark.parametrize("commit_type", title_check.COMMIT_TYPES)
def test_title_check_component(commit_type) -> None:
    title = f"{commit_type}(python): test"
    commit = title_check.matches_commit_format(root, title)
    assert not commit.failed_validation_reasons
    assert commit.category == commit_type
    assert commit.components == ["python"]
    assert not commit.breaking_change
    assert commit.subject == "test"


@pytest.mark.parametrize("commit_type", title_check.COMMIT_TYPES)
def test_title_check_multi(commit_type) -> None:
    title = f"{commit_type}(c,format,python)!: test"
    commit = title_check.matches_commit_format(root, title)
    assert not commit.failed_validation_reasons
    assert commit.category == commit_type
    assert commit.components == ["c", "format", "python"]
    assert commit.breaking_change
    assert commit.subject == "test"

    title = f"{commit_type}!(c,format,python): test"
    commit = title_check.matches_commit_format(root, title)
    assert not commit.failed_validation_reasons
    assert commit.category == commit_type
    assert commit.components == ["c", "format", "python"]
    assert commit.breaking_change
    assert commit.subject == "test"


@pytest.mark.parametrize("commit_type", title_check.COMMIT_TYPES)
def test_title_check_nested(commit_type) -> None:
    title = f"{commit_type}(c/driver,dev/release)!: test"
    commit = title_check.matches_commit_format(root, title)
    assert not commit.failed_validation_reasons
    assert commit.category == commit_type
    assert commit.components == ["c/driver", "dev/release"]
    assert commit.breaking_change
    assert commit.subject == "test"


@pytest.mark.parametrize(
    "msg",
    [
        "feat:",
        "unknown: foo",
        "feat(): test",
        "feat()!: test",
        "feat!(c)!: test",
        "feat(nonexistent): test",
        "feat(c,): test",
        "feat(c ): test",
        "feat( c): test",
        "feat(a#): test",
    ],
)
def test_title_check_bad(msg: str) -> None:
    commit = title_check.matches_commit_format(root, msg)
    assert commit.failed_validation_reasons


def test_list_commits(repo: pygit2.Repository) -> None:
    # the base rev is not included
    commits = changelog.list_commits(
        repo,
        "2360993884e6f82a6da9080d9fcd0dcf8c362b1d",
        "8f6ffe5bd1ee5667b5626f91fc3f928f93ae94cd",
    )
    assert len(commits) == 2
    assert (
        commits[0].subject
        == "bump com.uber.nullaway:nullaway from 0.12.2 to 0.12.3 in /java (#2417)"
    )
    assert (
        commits[1].subject
        == "bump golang.org/x/tools from 0.28.0 to 0.29.0 in /go/adbc (#2419)"
    )


@pytest.mark.parametrize("ref", ["HEAD", "2360993884e6f82a6da9080d9fcd0dcf8c362b1d"])
def test_get_commit(repo: pygit2.Repository, ref: str) -> None:
    assert changelog.get_commit(repo, ref) is not None


def test_format_commit(repo: pygit2.Repository) -> None:
    commits = changelog.list_commits(
        repo,
        "196522bb2f11665c7b6e0d1ed98da174315a19d9",
        "63bb903b7ddc7730beaa3d092dc5808632cd4b08",
    )
    assert len(commits) == 1

    formatted = changelog.format_commit(commits[0])
    assert (
        formatted
        == "**c**: don't use sketchy cast to test backwards compatibility (#2425)"
    )

    commits = changelog.list_commits(
        repo,
        "5299ea01ab31b276c27059d82efdbdead22029e9",
        "460937c76b923420d07d5bcfd29166c80eb45d80",
    )
    assert len(commits) == 1

    formatted = changelog.format_commit(commits[0])
    assert formatted == (
        "⚠️ **java/driver-manager**: support loading "
        "AdbcDrivers from the ServiceLoader (#1475)"
    )


def test_format_section(repo: pygit2.Repository) -> None:
    assert changelog.format_section("Breaking Changes", []) == []

    commits = changelog.list_commits(
        repo,
        "5299ea01ab31b276c27059d82efdbdead22029e9",
        "460937c76b923420d07d5bcfd29166c80eb45d80",
    )
    assert len(commits) == 1

    assert changelog.format_section("Breaking Changes", commits) == [
        "### Breaking Changes",
        "",
        f"- {changelog.format_commit(commits[0])}",
        "",
    ]
