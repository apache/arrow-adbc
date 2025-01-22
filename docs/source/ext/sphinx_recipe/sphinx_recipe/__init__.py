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

"""A directive for code recipes with a literate programming style."""

import typing
from pathlib import Path

import docutils
from docutils.parsers.rst import directives
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles
from sphinx.util.typing import OptionSpec

__all__ = ["setup"]


class SourceLine(typing.NamedTuple):
    content: str
    lineno: int


class SourceFragment(typing.NamedTuple):
    kind: str
    lines: list[SourceLine]


PREAMBLE = "Recipe source: `{name} <{url}>`_"


class RecipeDirective(SphinxDirective):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    option_spec: OptionSpec = {
        "language": directives.unchanged_required,
        "prose-prefix": directives.unchanged_required,
    }

    @staticmethod
    def default_prose_prefix(language: str) -> str:
        return {
            "cpp": "///",
            "go": "///",
            "python": "#:",
        }.get(language, "#:")

    def run(self):
        rel_filename, filename = self.env.relfn2path(self.arguments[0])
        self.env.note_dependency(rel_filename)
        self.env.note_dependency(__file__)

        language = self.options.get("language", "python")
        prefix = self.options.get("prose-prefix", self.default_prose_prefix(language))

        # --- Split the source into runs of prose or code

        fragments = []

        fragment = []
        fragment_type = None
        state = "before"
        lineno = 1
        for line in open(filename):
            if state == "before":
                if "RECIPE STARTS HERE" in line:
                    state = "reading"
            elif state == "reading":
                if line.strip().startswith(prefix):
                    line_type = "prose"
                    # Remove prefix and next whitespace
                    line = line.lstrip()[len(prefix) + 1 :]
                else:
                    line_type = "code"

                if line_type != fragment_type:
                    if fragment:
                        fragments.append(
                            SourceFragment(kind=fragment_type, lines=fragment)
                        )
                        fragment = []
                    fragment_type = line_type

                # Skip blank code lines
                if line_type != "code" or line.strip():
                    # Remove trailing newline
                    fragment.append(SourceLine(content=line[:-1], lineno=lineno))

            lineno += 1

        if fragment:
            fragments.append(SourceFragment(kind=fragment_type, lines=fragment))

        # --- Generate the final reST as a whole and parse it
        # That way, section hierarchy works properly

        generated_lines = []

        # Link to the source on GitHub
        repo_url_template = self.env.config.recipe_repo_url_template
        if repo_url_template is not None:
            repo_url = repo_url_template.format(rel_filename=rel_filename)
            generated_lines.append(
                PREAMBLE.format(
                    name=Path(rel_filename).name,
                    url=repo_url,
                )
            )

        # Paragraph break
        generated_lines.append("")

        for fragment in fragments:
            if fragment.kind == "prose":
                generated_lines.extend([line.content for line in fragment.lines])
                generated_lines.append("")
            elif fragment.kind == "code":
                line_min = fragment.lines[0].lineno
                line_max = fragment.lines[-1].lineno
                lines = [
                    f".. literalinclude:: {self.arguments[0]}",
                    f"   :language: {language}",
                    "   :linenos:",
                    "   :lineno-match:",
                    f"   :lines: {line_min}-{line_max}",
                    "",
                ]
                generated_lines.extend(lines)
            else:
                raise RuntimeError("Unknown fragment kind")

        parsed = docutils.nodes.Element()
        nested_parse_with_titles(
            self.state,
            StringList(generated_lines, source=""),
            parsed,
        )
        return parsed.children


def setup(app) -> None:
    app.add_config_value(
        "recipe_repo_url_template",
        default=None,
        rebuild="html",
        types=str,
    )
    app.add_directive("recipe", RecipeDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
