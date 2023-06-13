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

import importlib
from pathlib import Path

import pytest


def pytest_generate_tests(metafunc) -> None:
    root = (Path(__file__).parent.parent / "python/recipe/").resolve()
    recipes = root.rglob("*.py")
    metafunc.parametrize(
        "recipe", [pytest.param(path, id=path.stem) for path in recipes]
    )


def test_cookbook_recipe(recipe: Path) -> None:
    spec = importlib.util.spec_from_file_location(f"cookbook.{recipe.stem}", recipe)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
