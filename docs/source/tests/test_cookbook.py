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
    params = []
    for root in (
        (Path(__file__).parent.parent / "cpp/recipe_driver/").resolve(),
        (Path(__file__).parent.parent / "python/recipe/").resolve(),
    ):
        recipes = root.rglob("*.py")
        params.extend(pytest.param(path, id=path.stem) for path in recipes)
    metafunc.parametrize("recipe", params)


def test_cookbook_recipe(recipe: Path, capsys: pytest.CaptureFixture) -> None:
    spec = importlib.util.spec_from_file_location("__main__", recipe)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    output = recipe.with_suffix(".stdout.txt")
    if output.is_file():
        with output.open("r") as source:
            expected = [line for line in source.read().strip().splitlines() if line]

        captured = [
            line for line in capsys.readouterr().out.strip().splitlines() if line
        ]
        assert captured == expected
