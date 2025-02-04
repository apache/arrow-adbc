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
import os
import subprocess
import typing
from pathlib import Path

import pytest


class Recipe(typing.NamedTuple):
    py_source: Path | None
    executable: Path | None
    output: Path | None


def pytest_generate_tests(metafunc) -> None:
    params = []
    for root in (
        (Path(__file__).parent.parent / "cpp/recipe_driver/").resolve(),
        (Path(__file__).parent.parent / "python/recipe/").resolve(),
    ):
        recipes = root.rglob("*.py")
        for path in recipes:
            output = path.with_suffix(path.suffix + ".stdout.txt")
            if output.is_file():
                recipe = Recipe(py_source=path, executable=None, output=output)
            else:
                recipe = Recipe(py_source=path, executable=None, output=None)
            params.append(pytest.param(recipe, id=f"py_{path.stem}"))

    # Find C++ examples with output
    cpp_bin = os.environ.get("ADBC_CPP_RECIPE_BIN")
    if cpp_bin:
        cpp_bin = Path(cpp_bin).resolve()
        recipes = (Path(__file__).parent.parent / "cpp/").resolve().rglob("*.cc")
        for path in recipes:
            output = path.with_suffix(path.suffix + ".stdout.txt")
            if not output.is_file():
                continue

            name = f"recipe-{path.stem}"
            executable = cpp_bin / name
            if not executable.is_file():
                raise ValueError(f"Not found: {executable} for {path}")

            recipe = Recipe(py_source=None, executable=executable, output=output)
            params.append(pytest.param(recipe, id=f"cpp_{path.stem}"))

    metafunc.parametrize("recipe", params)


def test_cookbook_recipe(recipe: Recipe, capsys: pytest.CaptureFixture) -> None:
    if recipe.py_source:
        spec = importlib.util.spec_from_file_location("__main__", recipe.py_source)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        if recipe.output:
            with recipe.output.open("r") as source:
                expected = [line for line in source.read().strip().splitlines() if line]

            captured = [
                line for line in capsys.readouterr().out.strip().splitlines() if line
            ]
            assert captured == expected
    elif recipe.executable:
        assert recipe.output is not None

        with recipe.output.open("r") as source:
            expected = [line for line in source.read().strip().splitlines() if line]

        output = subprocess.check_output(recipe.executable, text=True)
        captured = [line for line in output.strip().splitlines() if line]

        assert captured == expected
    else:
        assert False
