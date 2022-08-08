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

from Cython.Build import cythonize
from setuptools import Extension


def build(setup_kwargs):
    """Configure build for Poetry."""
    library_dirs = os.environ.get("ADBC_LIBRARY_DIRS", "")
    if library_dirs:
        library_dirs = library_dirs.split(":")
    else:
        library_dirs = []
    setup_kwargs["ext_modules"] = cythonize(
        Extension(
            name="adbc_driver_postgres._lib",
            include_dirs=["../../"],
            libraries=["adbc_driver_postgres"],
            library_dirs=library_dirs,
            language="c",
            sources=[
                "adbc_driver_postgres/_lib.pyx",
            ],
        ),
    )
    setup_kwargs["zip_safe"] = False
