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

import argparse
import re
import sys
import typing
from pathlib import Path

COMMIT_TYPES = {
    "build",
    "chore",
    "ci",
    "docs",
    "feat",
    "fix",
    "perf",
    "refactor",
    "revert",
    "style",
    "test",
}


def matches_commit_format(root: Path, title: str) -> typing.List[str]:
    """Check a title and return a list of reasons why it's invalid."""
    if not root.is_dir():
        return [f"Invalid root: must be a directory: {root}"]

    # Relax the initial regex a bit, do more friendly validation below
    commit_type = "([a-z]+)"
    scope = r"(?:\(([^\)]*)\))?"
    delimiter = "!?:"
    subject = " (.+)"
    commit = re.compile(f"^{commit_type}{scope}{delimiter}{subject}$")
    valid_component = re.compile(r"^[a-zA-Z0-9_/\-\.]+$")

    m = commit.match(title)
    if m is None:
        return [
            "Format is incorrect, see https://www.conventionalcommits.org/en/v1.0.0/"
        ]

    reasons = []
    commit_type = m.group(1)
    if commit_type not in COMMIT_TYPES:
        reasons.append(f"Invalid commit type: {commit_type}")

    components = m.group(2)
    if components is not None:
        if not components.strip():
            reasons.append("Invalid components: must not be empty")
        else:
            components = components.split(",")
            for component in components:
                if component != component.strip():
                    reasons.append(
                        f"Invalid component: must have no trailing space: {component}"
                    )
                elif not valid_component.match(component):
                    reasons.append(
                        "Invalid component: must be alphanumeric "
                        f"plus [.-/]: {component}"
                    )
                elif component != "format" and not Path(component).exists():
                    reasons.append(
                        "Invalid component: must reference a file "
                        f"or directory in the repo: {component}"
                    )

    subject = m.group(3)
    if subject.strip() != subject:
        reasons.append(f"Invalid subject: must have no trailing space: {subject}")
    if subject.strip().endswith("."):
        reasons.append(f"Invalid subject: must not end in a period: {subject}")

    return reasons


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="The root of the repository")
    parser.add_argument("title", help="The PR title to check")

    args = parser.parse_args()

    print(f'PR title: "{args.title}"')

    reasons = matches_commit_format(args.root, args.title)
    if not reasons:
        print("Title is valid")
        return 0

    print("Title is invalid:")
    for reason in reasons:
        print("-", reason)
    return 1


if __name__ == "__main__":
    sys.exit(main())
