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

import os
import shutil
import sys
from collections import namedtuple
from pathlib import Path

from setuptools import Extension, setup

source_root = Path(__file__).parent
repo_root = source_root.joinpath("../../")

# ------------------------------------------------------------
# Resolve C++ Sources

FileToCopy = namedtuple("FileToCopy", ["source", "dest_dir"])
files_to_copy = [
    FileToCopy("c/include/arrow-adbc/adbc.h", "arrow-adbc"),
    FileToCopy("c/driver_manager/adbc_driver_manager.cc", ""),
    FileToCopy("c/driver_manager/current_arch.h", ""),
    FileToCopy("c/include/arrow-adbc/adbc_driver_manager.h", "arrow-adbc"),
    FileToCopy("c/vendor/backward/backward.hpp", ""),
    FileToCopy("c/vendor/toml++/toml.hpp", "toml++"),
]

for file_to_copy in files_to_copy:
    target_filename = file_to_copy.source.split("/")[-1]
    source = repo_root.joinpath(file_to_copy.source).resolve()
    target_dir = source_root.joinpath(
        "adbc_driver_manager", file_to_copy.dest_dir
    ).resolve()
    target = target_dir.joinpath(target_filename).resolve()
    if source.is_file():
        # In-tree build/creating an sdist: copy from project root to local file
        # so that setuptools isn't confused
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, target)
    elif not target.is_file():
        # Out-of-tree build missing the C++ source files
        raise FileNotFoundError(str(target))
    # Else, when building from sdist, the target will exist but not the source

# ------------------------------------------------------------
# Resolve Version (miniver)


def get_version(pkg_path):
    """
    Load version.py module without importing the whole package.

    Template code from miniver.
    """
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("version", os.path.join(pkg_path, "_version.py"))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__


version = get_version("adbc_driver_manager")

# ------------------------------------------------------------
# Resolve compiler flags

build_type = os.environ.get("ADBC_BUILD_TYPE", "release")

if sys.platform == "win32":
    extra_compile_args = ["/std:c++17", "/DADBC_EXPORTING", "/D_CRT_SECURE_NO_WARNINGS"]
    if build_type == "debug":
        extra_compile_args.extend(["/DEBUG:FULL"])
    extra_link_args = ["shell32.lib", "uuid.lib", "advapi32.lib"]
else:
    extra_compile_args = ["-std=c++17"]
    if build_type == "debug":
        # Useful to step through driver manager code in GDB
        extra_compile_args.extend(["-ggdb", "-Og"])
    extra_link_args = []

# ------------------------------------------------------------
# Setup

setup(
    ext_modules=[
        Extension(
            name="adbc_driver_manager._backward",
            extra_compile_args=extra_compile_args,
            include_dirs=[str(source_root.joinpath("adbc_driver_manager").resolve())],
            language="c++",
            sources=[
                "adbc_driver_manager/_backward.pyx",
            ],
        ),
        Extension(
            name="adbc_driver_manager._lib",
            extra_compile_args=extra_compile_args,
            extra_link_args=extra_link_args,
            include_dirs=[str(source_root.joinpath("adbc_driver_manager").resolve())],
            language="c++",
            sources=[
                "adbc_driver_manager/_blocking_impl.cc",
                "adbc_driver_manager/_lib.pyx",
                "adbc_driver_manager/adbc_driver_manager.cc",
            ],
        ),
        Extension(
            name="adbc_driver_manager._reader",
            extra_compile_args=extra_compile_args,
            include_dirs=[str(source_root.joinpath("adbc_driver_manager").resolve())],
            language="c++",
            sources=[
                "adbc_driver_manager/_reader.pyx",
            ],
        ),
    ],
    version=version,
)
