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
import sys
from pathlib import Path

from . import parser as recipe_parser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("recipes", nargs="+", type=Path, help="Recipe files to update")

    args = parser.parse_args()

    updated = False
    for path in args.recipes:
        syntax = recipe_parser.LANGUAGES[path.suffix]
        with path.open("r") as source:
            recipe = recipe_parser.parse_recipe_to_fragments(source, syntax=syntax)

        stdout = [line for line in recipe.stdout if line]
        if not stdout:
            continue

        target = path.with_suffix(".stdout.txt")
        if target.is_file():
            with target.open("r") as source:
                if source.read().strip() == "\n".join(stdout).strip():
                    print(path, "is up to date")
                    continue

        with target.open("w") as sink:
            sink.writelines(stdout)
        print(path, "updated")
        updated = True

    return 1 if updated else 0


if __name__ == "__main__":
    sys.exit(main())
