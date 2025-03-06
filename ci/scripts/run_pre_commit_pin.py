#!/usr/bin/env python
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

"""Validate that pre-commit repos are pinned to a commit hash."""

import argparse
import re
import subprocess
import sys
from pathlib import Path

from ruamel.yaml import YAML


def resolve_repo(repo: str, tag: str, cache: dict[tuple[str, str], str]) -> str:
    key = (repo, tag)
    if key in cache:
        return cache[key]

    ls_remote = subprocess.check_output(
        [
            "git",
            "ls-remote",
            repo,
            f"refs/tags/{tag}",
        ],
        text=True,
    )
    sha, _ = ls_remote.strip().split(maxsplit=1)
    cache[key] = sha
    return sha


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    sha_re = re.compile(r"^[0-9a-f]{40}$")
    ret = 0
    yaml = YAML(typ="rt")
    resolved = {}

    for path in args.files:
        print("Checking", path)
        with path.open() as source:
            data = yaml.load(source)

        if path.name == ".pre-commit-config.yaml":
            for repo in data["repos"]:
                # import code; code.interact(local=locals())
                # return 1
                rev = repo.get("rev")
                if rev is None:
                    if repo["repo"] != "local":
                        print("repo is missing `rev:`")
                        print("- repo:", repo["repo"])
                        ret += 1
                elif not sha_re.match(rev):
                    sha = resolve_repo(repo["repo"], rev, resolved)
                    print("`rev` should be a commit hash:")
                    print("- repo:", repo["repo"])
                    print("  rev:", sha, " #", repo["rev"])
                    ret += 1
        else:
            # GitHub Actions
            for key, job in data["jobs"].items():
                if "steps" not in job:
                    continue

                for step in job["steps"]:
                    if "uses" not in step:
                        continue
                    action, _, rev = step["uses"].partition("@")
                    repo = "/".join(action.split("/", maxsplit=2)[:2])
                    if not sha_re.match(rev):
                        if (
                            repo.startswith("actions/")
                            or repo.startswith("apache/")
                            or repo.startswith("github/")
                        ):
                            # GitHub-provided actions don't _have_ to be pinned
                            continue

                        sha = resolve_repo(f"https://github.com/{repo}", rev, resolved)
                        print(step["uses"], "should be pinned:")
                        print(f"{action}@{sha}  # {rev}")
                        ret += 1

    return ret


if __name__ == "__main__":
    sys.exit(main())
