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
from pathlib import Path

from setuptools import Extension, setup

source_root = Path(__file__).parent
repo_root = source_root.joinpath("../../")

# ------------------------------------------------------------
# Resolve C++ Sources

sources = [
    "adbc.h",
    "c/driver_manager/adbc_driver_manager.cc",
    "c/driver_manager/adbc_driver_manager.h",
    "c/vendor/backward/backward.hpp",
]

for source in sources:
    target_filename = source.split("/")[-1]
    source = repo_root.joinpath(source).resolve()
    target = source_root.joinpath("adbc_driver_manager", target_filename).resolve()
    if source.is_file():
        # In-tree build/creating an sdist: copy from project root to local file
        # so that setuptools isn't confused
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
else:
    extra_compile_args = ["-std=c++17"]
    if build_type == "debug":
        # Useful to step through driver manager code in GDB
        extra_compile_args.extend(["-ggdb", "-Og"])

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
