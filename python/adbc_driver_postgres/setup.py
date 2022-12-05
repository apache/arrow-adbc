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
from pathlib import Path

from setuptools import setup

source_root = Path(__file__).parent
repo_root = source_root.joinpath("../../")

# ------------------------------------------------------------
# Resolve Shared Library

library = os.environ.get("ADBC_POSTGRES_LIBRARY")
target = source_root.joinpath(
    "./adbc_driver_postgres/libadbc_driver_postgres.so"
).resolve()

if not library:
    if os.environ.get("_ADBC_IS_SDIST", "").strip().lower() in ("1", "true"):
        print("Building sdist, not requiring ADBC_POSTGRES_LIBRARY")
    elif target.is_file():
        print("Driver already exists (but may be stale?), continuing")
    else:
        raise ValueError("Must provide ADBC_POSTGRES_LIBRARY")
else:
    shutil.copy(library, target)

# ------------------------------------------------------------
# Resolve Version (miniver)


def get_version_and_cmdclass(pkg_path):
    """
    Load version.py module without importing the whole package.

    Template code from miniver.
    """
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("version", os.path.join(pkg_path, "_version.py"))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__, module.get_cmdclass(pkg_path)


version, cmdclass = get_version_and_cmdclass("adbc_driver_postgres")

# ------------------------------------------------------------
# Setup

setup(
    cmdclass=cmdclass,
    version=version,
)
