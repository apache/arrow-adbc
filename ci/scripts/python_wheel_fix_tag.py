#!/usr/bin/env python3
#
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

# Fix the platform tag in macOS/Windows wheels because
# delocate/delvewheel don't do the right thing.

import argparse
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--plat-name",
        default=None,
        help=f"Tag to use (defaults to {sysconfig.get_platform()})",
    )
    parser.add_argument("wheel", nargs="+", help="Wheels to retag")
    args = parser.parse_args()

    plat_tag = args.plat_name or sysconfig.get_platform()
    plat_tag = plat_tag.replace("-", "_")
    plat_tag = plat_tag.replace(".", "_")
    print("Using platform tag: ", plat_tag)

    for wheel in args.wheel:
        with tempfile.TemporaryDirectory() as dest:
            dest = Path(dest)
            wheel = Path(wheel).resolve()
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "wheel",
                    "unpack",
                    "--dest",
                    str(dest),
                    str(wheel),
                ]
            )

            (root,) = list(dest.glob("*"))

            metadata_files = list(root.rglob("*.dist-info/WHEEL"))
            if len(metadata_files) != 1:
                raise ValueError(f"Expected one WHEEL file, found: {metadata_files}")

            with metadata_files[0].open() as source:
                contents = source.read()

            new_contents = contents.replace(
                "Root-Is-Purelib: true", "Root-Is-Purelib: false"
            )
            new_contents = new_contents.replace(
                "Tag: py3-none-any", f"Tag: py3-none-{plat_tag}"
            )

            if new_contents == contents:
                print("No changes made (wheel already properly tagged?)")
                continue

            with metadata_files[0].open("w") as sink:
                sink.write(new_contents)

            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "wheel",
                    "pack",
                    "--dest-dir",
                    str(wheel.parent),
                    str(root),
                ]
            )
            wheel.unlink()


if __name__ == "__main__":
    main()
