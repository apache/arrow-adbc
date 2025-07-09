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

"""Misc directives for the ADBC docs."""

import collections
import dataclasses
import functools
import itertools
import typing
from pathlib import Path

import docutils
import sphinx
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles
from sphinx.util.typing import OptionSpec

LOGGER = sphinx.util.logging.getLogger(__name__)

# conda-forge is handled specially
_REPO_TO_LANGUAGE = {
    "CRAN": "R",
    "crates.io": "Rust",
    "Go": "Go",
    "Maven": "Java",
    "NuGet": "C#",
    "RubyGems": "Ruby",
    "R-multiverse": "R",
    "PyPI": "Python",
}

_LANGUAGE_TO_KEY = {
    "C/C++": "cpp",
    "C#": "csharp",
}


@dataclasses.dataclass(frozen=True)
class DriverStatus:
    vendor: str
    implementation: str
    status: typing.Literal["Experimental", "Beta", "Stable"]
    packages: typing.List[typing.Tuple[str, str, str]]  # (repo, package, URL)

    @property
    def badge_type(self) -> str:
        if self.status == "Experimental":
            return "danger"
        elif self.status == "Beta":
            return "warning"
        elif self.status == "Stable":
            return "success"
        else:
            raise ValueError(f"Unknown status {self.status} for {self.implementation}")


@functools.cache
def _driver_status(path: Path) -> DriverStatus:
    # we could pull in a full markdown parser, but for now just munge the text
    meta: typing.Dict[str, str] = {}
    packages = []
    with path.open() as source:
        for line in source:
            if "img.shields.io" in line:
                before, _, after = line.partition("img.shields.io")
                tag = before[before.index("![") + 2 : before.index("]")].strip()
                key, _, value = tag.partition(": ")
                key = key.strip()
                value = value.strip()

                if key.lower() in {"vendor", "implementation", "status"}:
                    meta[key.lower()] = value
                else:
                    repo = key
                    url = after[after.rfind("(") + 1 : after.rfind(")")].strip()
                    packages.append((repo, value, url))
    return DriverStatus(**meta, packages=packages)


def driver_status(path: Path) -> DriverStatus:
    return _driver_status(path.resolve())


class DriverInstallationDirective(SphinxDirective):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    option_spec: OptionSpec = {}

    def run(self):
        rel_filename, filename = self.env.relfn2path(self.arguments[0])
        self.env.note_dependency(rel_filename)

        path = Path(filename).resolve()
        status = driver_status(path)

        generated_lines = []

        if not status.packages:
            generated_lines.append("No packages available; install from source.")
        else:
            generated_lines.append(".. tab-set::")

            # language : list of (repo, package, url)
            languages = collections.defaultdict(list)

            for i, (repo, package, url) in enumerate(status.packages):
                language = None
                if repo == "conda-forge":
                    if package.startswith("lib"):
                        language = "C/C++"
                    else:
                        language = "Python"
                else:
                    language = _REPO_TO_LANGUAGE.get(repo)

                if language is None:
                    LOGGER.warning(
                        f"Unknown language mapping for package repo {repo}",
                        type="adbc_misc",
                    )
                    continue

                languages[language].append((repo, package, url))

            if "Go" not in languages:
                languages["Go"] = []

            for language, packages in sorted(languages.items(), key=lambda x: x[0]):
                generated_lines.append("")
                generated_lines.append(f"   .. tab-item:: {language}")
                generated_lines.append(
                    f"      :sync: {_LANGUAGE_TO_KEY.get(language, language.lower())}"
                )
                generated_lines.append("")

                for repo, package, url in sorted(
                    packages, key=lambda x: (x[0].lower(), x[1])
                ):
                    generated_lines.append(
                        f"      Install `{package} <{url}>`_ from {repo}:"
                    )
                    generated_lines.append("")
                    if repo == "conda-forge":
                        generated_lines.append("      .. code-block:: shell")
                        generated_lines.append("")
                        generated_lines.append(f"         mamba install {package}")
                    elif repo == "crates.io":
                        generated_lines.append("      .. code-block:: shell")
                        generated_lines.append("")
                        generated_lines.append(f"         cargo add {package}")
                    elif repo == "CRAN":
                        generated_lines.append("      .. code-block:: r")
                        generated_lines.append("")
                        generated_lines.append(
                            f'         install.packages("{package}")'
                        )
                    elif repo == "Go":
                        generated_lines.append("      .. code-block:: shell")
                        generated_lines.append("")
                        generated_lines.append(f"         go get {package}")
                    elif repo == "NuGet":
                        generated_lines.append("      .. code-block:: shell")
                        generated_lines.append("")
                        generated_lines.append(f"         dotnet package add {package}")
                    elif repo == "PyPI":
                        generated_lines.append("      .. code-block:: shell")
                        generated_lines.append("")
                        generated_lines.append(f"         pip install {package}")
                    elif repo == "R-multiverse":
                        generated_lines.append("      .. code-block:: r")
                        generated_lines.append("")
                        generated_lines.append(
                            f'         install.packages("{package}", '
                            'repos = "https://community.r-multiverse.org")'
                        )
                    else:
                        LOGGER.warning(f"Unknown package repo {repo}", type="adbc_misc")
                        continue
                    generated_lines.append("")

                if not packages:
                    if language == "Go":
                        generated_lines.append(
                            "      Install the C/C++ driver, "
                            "then use the Go driver manager.  "
                            "Requires CGO."
                        )
                        generated_lines.append("")
                        generated_lines.append("      .. code-block:: shell")
                        generated_lines.append("")
                        generated_lines.append(
                            "         go get "
                            "github.com/apache/arrow-adbc/go/adbc/drivermgr"
                        )
                    else:
                        LOGGER.warning(
                            f"No packages and unknown language {language}",
                            type="adbc_misc",
                        )

        if status.implementation in {"C/C++", "C#", "Go", "Rust"}:
            generated_lines.append("")
            generated_lines.append(
                "Additionally, the driver may be used from C/C++, C#, GLib, "
                "Go, R, Ruby, and Rust via the driver manager."
            )

        parsed = docutils.nodes.Element()
        nested_parse_with_titles(
            self.state,
            StringList(generated_lines, source=""),
            parsed,
        )
        return parsed.children


class DriverStatusDirective(SphinxDirective):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    option_spec: OptionSpec = {}

    def run(self):
        rel_filename, filename = self.env.relfn2path(self.arguments[0])
        self.env.note_dependency(rel_filename)

        path = Path(filename).resolve()
        status = driver_status(path)

        generated_lines = [
            f":bdg-primary:`Language: {status.implementation}`",
            f":bdg-ref-{status.badge_type}:`Status: {status.status} <driver-status>`",
        ]

        parsed = docutils.nodes.Element()
        nested_parse_with_titles(
            self.state,
            StringList(generated_lines, source=""),
            parsed,
        )
        return parsed.children


class DriverStatusTableDirective(SphinxDirective):
    has_content = True
    required_arguments = 0
    optional_arguments = 0
    option_spec: OptionSpec = {}

    def run(self):
        table = []
        for line in self.content:
            if "=>" in line:
                xref, _, path = line.partition("=>")
                xref = xref.strip()
                path = path.strip()
            else:
                xref = None
                path = line.strip()

            if "[#" in path:
                footnote = path[path.index("[#") + 2 : -1].strip()
                path = path[: path.index("[#")].strip()
            else:
                footnote = None

            rel_filename, filename = self.env.relfn2path(path)
            self.env.note_dependency(rel_filename)

            path = Path(filename).resolve()
            status = driver_status(path)
            table.append((status, xref, footnote))

        table.sort(key=lambda x: (x[0].vendor, x[0].implementation))

        generated_lines = [
            ".. list-table::",
            "   :header-rows: 1",
            "",
            "   * - Vendor",
            "     - Implementation",
            "     - :ref:`driver-status`",
            "     - Packages [#packages]_",
            "",
        ]
        for row in table:
            if row[1]:
                generated_lines.append(f"   * - :doc:`{row[0].vendor} <{row[1]}>`")
            else:
                generated_lines.append(f"   * - {row[0].vendor}")

            if row[2]:
                generated_lines[-1] += f" [#{row[2]}]_"

            generated_lines.append(f"     - {row[0].implementation}")
            generated_lines.append(f"     - {row[0].status}")

            generated_lines.append("     -")
            packages = itertools.groupby(
                sorted(row[0].packages, key=lambda x: x[0].lower()),
                key=lambda x: x[0],
            )
            for repo, group in packages:
                group = list(group)
                if generated_lines[-1][-1] == "-":
                    generated_lines[-1] += " "
                else:
                    generated_lines[-1] += ", "

                if len(group) == 1:
                    generated_lines[-1] += f"`{repo} <{group[0][2]}>`__"
                else:
                    links = ", ".join(f"`{pkg[1]} <{pkg[2]}>`__" for pkg in group)
                    generated_lines[-1] += f"{repo} ({links})"
            generated_lines.append("")

        generated_lines.extend(
            [
                "",
                ".. [#packages] This lists only packages available in package repositories.  However, as noted above, many of these drivers can be used from languages not listed via the driver manager, even if a package is not yet available.",  # noqa:E501
            ]
        )

        parsed = docutils.nodes.Element()
        nested_parse_with_titles(
            self.state,
            StringList(generated_lines, source=""),
            parsed,
        )
        return parsed.children


def setup(app) -> None:
    app.add_directive("adbc_driver_installation", DriverInstallationDirective)
    app.add_directive("adbc_driver_status", DriverStatusDirective)
    app.add_directive("adbc_driver_status_table", DriverStatusTableDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
