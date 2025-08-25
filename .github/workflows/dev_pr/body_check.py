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

PING_RE = re.compile(r"@([a-zA-Z0-9\-]+)")
IGNORED_USERNAMES = {"dependabot"}


def check_pr_body(body: str) -> typing.List[str]:
    """Check a PR body and return a list of reasons why it's invalid."""

    reasons = []
    matches = PING_RE.findall(body)
    for username in matches:
        if username in IGNORED_USERNAMES:
            continue
        reasons.append(f"Please don't ping {username} in the PR description")

    return reasons


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("body", help="The PR body to check")

    args = parser.parse_args()

    print(f'PR body: "{args.body}"')
    print("=" * 60)

    reasons = check_pr_body(args.body)
    if not reasons:
        print("PR body is valid")
        return 0

    print("PR body is invalid:")
    for reason in reasons:
        print("-", reason)
    return 1


if __name__ == "__main__":
    sys.exit(main())
