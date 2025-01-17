#!/usr/bin/env python3
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

"""Generate a changelog from our commit log."""

import argparse
import datetime
import sys
from pathlib import Path

import dotenv
import pygit2

from . import title_check


def display(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def get_commit(repo: pygit2.Repository, rev: str) -> pygit2.Oid:
    try:
        return repo.lookup_reference_dwim(rev).target
    except KeyError:
        return repo[rev].id


def list_commits(
    repo: pygit2.Repository, from_rev: str, to_rev: str
) -> list[title_check.Commit]:
    root = Path(repo.workdir)
    from_commit = get_commit(repo, from_rev)
    to_commit = get_commit(repo, to_rev)
    walker = repo.walk(to_commit, pygit2.GIT_SORT_TIME)
    walker.hide(from_commit)
    commits = []
    for commit in walker:
        title = commit.message.strip().split("\n")[0]
        commits.append(title_check.matches_commit_format(root, title))
    return commits


def format_commit(commit: title_check.Commit) -> str:
    components = ""
    warning = ""
    if commit.components:
        components = f"**{', '.join(commit.components)}**: "
    if commit.breaking_change:
        warning = "⚠️ "
    return f"{warning}{components}{commit.subject}"


def format_section(title: str, commits: list[title_check.Commit]) -> list[str]:
    if not commits:
        return []

    lines = [f"### {title}", ""]
    commits.sort(key=lambda commit: (commit.components, commit.subject))
    lines.extend(f"- {format_commit(commit)}" for commit in commits)
    lines.append("")
    return lines


def format_changelog(
    title: str, release: dict[str, str], commits: list[title_check.Commit]
) -> str:
    date = datetime.date.today().strftime("%Y-%m-%d")
    lines = [
        f"## {title} ({date})",
        "",
        "### Versions",
        "",
        f"- C/C++/GLib/Go/Python/Ruby: {release['VERSION_NATIVE']}",
        f"- C#: {release['VERSION_CSHARP']}",
        f"- Java: {release['VERSION_JAVA']}",
        f"- R: {release['VERSION_R']}",
        f"- Rust: {release['VERSION_RUST']}",
        "",
    ]

    breaking = [commit for commit in commits if commit.breaking_change]
    lines.extend(format_section("Breaking Changes", breaking))

    feat = [commit for commit in commits if commit.category == "feat"]
    lines.extend(format_section("New Features", feat))

    fix = [commit for commit in commits if commit.category == "fix"]
    lines.extend(format_section("Bugfixes", fix))

    docs = [commit for commit in commits if commit.category == "docs"]
    lines.extend(format_section("Documentation Improvements", docs))

    perf = [commit for commit in commits if commit.category == "perf"]
    lines.extend(format_section("Performance Improvements", perf))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("from_rev", help="The start revision.")
    parser.add_argument("to_rev", help="The end revision.")
    parser.add_argument("--name", required=True, help="The name of the release.")

    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent.parent.resolve()
    release = dotenv.dotenv_values(repo_root / "dev/release/versions.env")
    display("Opening repository at", repo_root)
    repo = pygit2.Repository(repo_root)

    commits = list_commits(repo, args.from_rev, args.to_rev)
    changelog = format_changelog(args.name, release, commits)
    print(changelog, end="")

    return 0


if __name__ == "__main__":
    sys.exit(main())
