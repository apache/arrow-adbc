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

"""Generate a Sphinx inventory for a TypeDoc HTML site.

TypeDoc outputs HTML pages whose URLs follow a predictable pattern based on
the reflection kind and the module.Name structure.  This script walks the
TypeDoc JSON output, derives the URL for each symbol, and writes a Sphinx
``objects.inv`` file that can be used with intersphinx.

Usage::

    python typedoc_inventory.py "ADBC JavaScript" "version" \\
        /path/to/typedoc/output javascript/api
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import sphinx.util.inventory
from fake_inventory import (
    FakeBuildEnvironment,
    FakeBuilder,
    FakeDomain,
    FakeDomainsContainer,
    FakeEnv,
    FakeObject,
)

# TypeDoc reflection kind constants
# https://typedoc.org/api/enums/Models.ReflectionKind.html
_KIND_MODULE = 2
_KIND_NAMESPACE = 4
_KIND_ENUM = 8
_KIND_ENUM_MEMBER = 16
_KIND_VARIABLE = 32
_KIND_FUNCTION = 64
_KIND_CLASS = 128
_KIND_INTERFACE = 256
_KIND_CONSTRUCTOR = 512
_KIND_PROPERTY = 1024
_KIND_METHOD = 2048
_KIND_ACCESSOR = 262144
_KIND_TYPE_ALIAS = 2097152

# Map TypeDoc kind → (Sphinx object type, HTML subdirectory)
# Subdirectory is None for members that live on their parent's page.
_KIND_INFO: dict[int, tuple[str, str | None]] = {
    _KIND_CLASS: ("jsclass", "classes"),
    _KIND_INTERFACE: ("jsinterface", "interfaces"),
    _KIND_ENUM: ("jsenum", "enums"),
    _KIND_VARIABLE: ("jsvariable", "variables"),
    _KIND_FUNCTION: ("jsfunction", "functions"),
    _KIND_TYPE_ALIAS: ("jstypealias", "types"),
    _KIND_NAMESPACE: ("jsnamespace", "modules"),
    _KIND_MODULE: ("jsmodule", "modules"),
    # Members live on the parent page
    _KIND_PROPERTY: ("jsproperty", None),
    _KIND_METHOD: ("jsmethod", None),
    _KIND_CONSTRUCTOR: ("jsconstructor", None),
    _KIND_ENUM_MEMBER: ("jsenummember", None),
    _KIND_ACCESSOR: ("jsaccessor", None),
}


def _walk(
    reflection: dict,
    base_url: str,
    objects: list[FakeObject],
    module_name: str = "",
    parent_page: str = "",
    parent_name: str = "",
) -> None:
    """Recursively walk the TypeDoc reflection tree and emit FakeObjects."""
    kind = reflection.get("kind", 0)
    name = reflection.get("name", "")

    if not name:
        return

    kind_info = _KIND_INFO.get(kind)

    if kind == _KIND_MODULE:
        # Descend into the module, passing its name as context
        for child in reflection.get("children", []):
            _walk(
                child,
                base_url,
                objects,
                module_name=name,
                parent_page="",
                parent_name="",
            )
        return

    if kind_info is None:
        # Unknown/uninteresting kind – still descend
        for child in reflection.get("children", []):
            _walk(child, base_url, objects, module_name, parent_page, parent_name)
        return

    sphinx_type, subdir = kind_info
    full_name = f"{parent_name}.{name}" if parent_name else name

    if subdir is not None:
        # Top-level symbol: has its own HTML page
        page = f"{base_url}{subdir}/{module_name}.{name}.html"
        anchor = ""
    else:
        # Member: fragment on the parent's page
        page = parent_page
        anchor = name.lower()

    objects.append(
        FakeObject(
            name=full_name,
            dispname=full_name,
            typ=sphinx_type,
            docname=page,
            anchor=anchor,
            prio=1,
        )
    )

    # Recurse into children (class/interface members, enum members, etc.)
    child_page = (
        page if subdir is None else f"{base_url}{subdir}/{module_name}.{name}.html"
    )
    for child in reflection.get("children", []):
        _walk(child, base_url, objects, module_name, child_page, full_name)


def make_fake_domains(root: Path, base_url: str) -> dict[str, FakeDomain]:
    """Parse TypeDoc JSON and return fake Sphinx domains."""
    if not base_url.endswith("/"):
        base_url += "/"

    json_path = root / "typedoc.json"
    if not json_path.exists():
        raise FileNotFoundError(
            f"TypeDoc JSON not found at {json_path}. Did you run TypeDoc?"
        )

    with open(json_path) as f:
        data = json.load(f)

    objects: list[FakeObject] = []
    for child in data.get("children", []):
        _walk(child, base_url, objects)

    return {"std": FakeDomain("std", objects=objects)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Sphinx objects.inv from TypeDoc JSON+HTML output."
    )
    parser.add_argument("project", help="Project name.")
    parser.add_argument("version", help="Project version.")
    parser.add_argument(
        "path",
        type=Path,
        help="Path to the TypeDoc output directory (must contain typedoc.json).",
    )
    parser.add_argument("url", help="Eventual base URL of the TypeDoc HTML site.")
    args = parser.parse_args()

    domains = make_fake_domains(args.path, args.url)
    config = FakeEnv(project=args.project, version=args.version)
    env = FakeBuildEnvironment(
        config=config, domains=FakeDomainsContainer.from_dict(domains)
    )

    output = args.path / "objects.inv"
    sphinx.util.inventory.InventoryFile.dump(
        str(output),
        env,
        FakeBuilder(),
    )
    print("Wrote", output)


if __name__ == "__main__":
    main()
