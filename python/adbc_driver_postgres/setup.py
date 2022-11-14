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
if not library:
    if os.environ.get("_ADBC_IS_SDIST", "").strip().lower() in ("1", "true"):
        print("Building sdist, not requiring ADBC_POSTGRES_LIBRARY")
    else:
        raise ValueError("Must provide ADBC_POSTGRES_LIBRARY")
else:
    target = source_root.joinpath(
        "./adbc_driver_postgres/libadbc_driver_postgres.so"
    ).resolve()
    shutil.copy(library, target)

# ------------------------------------------------------------
# Resolve Version

use_scm_version = False

git_dir = source_root.joinpath("../../.git").resolve()
target = source_root.joinpath("adbc_driver_postgres/_version.py").resolve()
if git_dir.is_dir():
    use_scm_version = {
        "root": git_dir.parent,
        "write_to": target,
    }
elif not target.is_file():
    # Out-of-tree build missing the generated version
    raise FileNotFoundError(str(target))
else:
    # Else, when building from sdist, _version.py will already exist
    # XXX: this is awful
    with target.open() as f:
        scope = {}
        exec(f.read(), scope)
        os.environ["SETUPTOOLS_SCM_PRETEND_VERSION"] = scope["__version__"]
    use_scm_version = True

# ------------------------------------------------------------
# Setup

setup(
    use_scm_version=use_scm_version,
)
