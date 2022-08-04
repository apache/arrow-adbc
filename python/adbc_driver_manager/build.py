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

from Cython.Build import cythonize
from setuptools import Extension


def build(setup_kwargs):
    """Configure build for Poetry."""
    setup_kwargs["ext_modules"] = cythonize(
        Extension(
            name="adbc_driver_manager._lib",
            extra_compile_args=["-ggdb", "-Og"],
            include_dirs=["../../", "../../c/driver_manager"],
            language="c++",
            sources=[
                "adbc_driver_manager/_lib.pyx",
                "../../c/driver_manager/adbc_driver_manager.cc",
            ],
        ),
    )
    setup_kwargs["zip_safe"] = False
